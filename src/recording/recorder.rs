//! The recorder sits between the event loop and the storage worker.
//!
//! It assigns deterministic `seq` ordering, builds normalized rows, and pushes
//! them through a **bounded** channel to the [`StorageWorker`]. The event loop
//! never blocks: `try_send` is used, and a full queue is surfaced as a
//! critical storage diagnostic + DEGRADED health rather than a silent drop.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, watch};
use tracing::error;
use uuid::Uuid;

use crate::binance::types::DepthUpdate;
use crate::orderbook::level::{price_str_to_ticks, quantity_str_to_ticks};
use crate::recording::metrics::{RecorderMetrics, StorageHealth, StorageState};
use crate::recording::session::SessionRecord;
use crate::recording::worker::{Record, RecordingConfig, StorageWorker};
use crate::storage::{
    ms_to_datetime, ns_to_datetime, LevelChangeRow, RawEventRow, SnapshotRow, Storage, TradeRow,
};

/// A normalized trade ready to be recorded.
#[derive(Debug, Clone)]
pub struct NewTrade {
    pub symbol: String,
    pub trade_id: u64,
    pub first_trade_id: Option<u64>,
    pub last_trade_id: Option<u64>,
    /// Price in integer ticks.
    pub price: u64,
    /// Quantity in integer ticks.
    pub quantity: u64,
    /// "BUY" or "SELL".
    pub aggressor_side: String,
    pub exchange_event_time_ms: u64,
    pub trade_time_ms: u64,
    pub local_receive_time_ns: u128,
    pub order_type: String,
}

/// Front-end for recording. Clone the sender via [`Recorder::spawn_sender`] is
/// unnecessary — callers share the `Arc<Recorder>`.
pub struct Recorder {
    pub session: SessionRecord,
    tx: mpsc::Sender<Record>,
    shutdown_tx: watch::Sender<bool>,
    pub metrics: Arc<Mutex<RecorderMetrics>>,
    pub health: Arc<Mutex<StorageHealth>>,
    queue_len: Arc<AtomicUsize>,
    seq: AtomicU64,
}

/// Handle used to await the storage worker and to trigger shutdown.
pub struct RecorderHandle {
    join: tokio::task::JoinHandle<()>,
    shutdown_tx: watch::Sender<bool>,
}

impl RecorderHandle {
    pub async fn join(self) -> anyhow::Result<()> {
        self.join.await?;
        Ok(())
    }
}

/// Start a recorder + storage worker over the given storage.
pub fn start_recorder(
    storage: Arc<dyn Storage>,
    session: SessionRecord,
    config: RecordingConfig,
) -> (Arc<Recorder>, RecorderHandle) {
    let (tx, rx) = mpsc::channel(config.queue_capacity);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let metrics = Arc::new(Mutex::new(RecorderMetrics::default()));
    let health = Arc::new(Mutex::new(StorageHealth::new()));
    let queue_len = Arc::new(AtomicUsize::new(0));

    let recorder = Arc::new(Recorder {
        session: session.clone(),
        tx,
        shutdown_tx: shutdown_tx.clone(),
        metrics: Arc::clone(&metrics),
        health: Arc::clone(&health),
        queue_len: Arc::clone(&queue_len),
        seq: AtomicU64::new(1),
    });

    let worker = StorageWorker::new(
        rx,
        storage,
        config,
        session.session_id,
        metrics,
        health,
        queue_len,
    );
    let join = tokio::spawn(run_worker(worker, shutdown_rx));

    (recorder, RecorderHandle { join, shutdown_tx })
}

async fn run_worker(mut worker: StorageWorker, mut shutdown_rx: watch::Receiver<bool>) {
    let mut timer = tokio::time::interval(worker.config().flush_interval);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            record = worker.recv() => {
                match record {
                    Some(record) => {
                        worker.enqueue(record);
                        if worker.threshold_reached() {
                            worker.flush_all().await;
                        }
                    }
                    None => {
                        worker.flush_all().await;
                        break;
                    }
                }
            }
            _ = timer.tick() => {
                worker.flush_all().await;
            }
            changed = shutdown_rx.changed() => {
                if changed.is_ok() && *shutdown_rx.borrow() {
                    // Drain anything already received, then final flush.
                    while let Ok(record) = worker.recv_try() {
                        worker.enqueue(record);
                    }
                    worker.flush_all().await;
                    break;
                }
            }
        }
    }
}

impl Recorder {
    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::SeqCst)
    }

    /// Record a raw Binance payload exactly as received.
    pub fn record_raw(
        &self,
        symbol: &str,
        stream_type: &str,
        payload: String,
        exchange_event_ms: u64,
        exchange_transaction_ms: Option<u64>,
        local_receive_ns: u128,
    ) {
        {
            let mut m = self.metrics.lock().unwrap();
            m.raw_received += 1;
            m.raw_bytes_received += payload.len() as u64;
        }
        let row = RawEventRow {
            session_id: self.session.session_id,
            seq: self.next_seq(),
            symbol: symbol.to_string(),
            stream_type: stream_type.to_string(),
            exchange_event_time: ms_to_datetime(exchange_event_ms),
            exchange_transaction_time: exchange_transaction_ms.map(ms_to_datetime),
            local_receive_time: ns_to_datetime(local_receive_ns),
            raw_payload: payload,
        };
        if !self.send(Record::Raw(row)) {
            self.metrics.lock().unwrap().raw_dropped += 1;
        }
    }

    /// Record a normalized trade.
    pub fn record_trade(&self, t: NewTrade) {
        self.metrics.lock().unwrap().trades_received += 1;
        let row = TradeRow {
            session_id: self.session.session_id,
            seq: self.next_seq(),
            symbol: t.symbol,
            trade_id: t.trade_id,
            first_trade_id: t.first_trade_id,
            last_trade_id: t.last_trade_id,
            price: t.price,
            quantity: t.quantity,
            aggressor_side: t.aggressor_side,
            exchange_event_time: ms_to_datetime(t.exchange_event_time_ms),
            trade_time: ms_to_datetime(t.trade_time_ms),
            local_receive_time: ns_to_datetime(t.local_receive_time_ns),
            order_type: t.order_type,
        };
        if !self.send(Record::Trade(row)) {
            self.metrics.lock().unwrap().trades_dropped += 1;
        }
    }

    /// Record all price-level changes of a depth event. One row per level;
    /// all rows share the same `seq` so they can be grouped back into the
    /// original event for replay.
    pub fn record_depth_event(&self, event: &DepthUpdate, local_receive_ns: u128) {
        self.metrics.lock().unwrap().depth_events_received += 1;
        let seq = self.next_seq();
        let symbol = event.symbol.clone();
        let exchange_event_time = ms_to_datetime(event.event_time);
        let exchange_transaction_time = ms_to_datetime(event.transaction_time);
        let local_receive_time = ns_to_datetime(local_receive_ns);

        let mut rows = Vec::with_capacity(event.bids.len() + event.asks.len());

        for (price_str, qty_str) in &event.bids {
            let (Ok(price), Ok(quantity)) = (
                price_str_to_ticks(price_str),
                quantity_str_to_ticks(qty_str),
            ) else {
                continue;
            };
            rows.push(LevelChangeRow {
                session_id: self.session.session_id,
                seq,
                symbol: symbol.clone(),
                exchange_event_time,
                exchange_transaction_time,
                local_receive_time,
                first_update_id: event.first_update_id,
                final_update_id: event.final_update_id,
                previous_final_update_id: event.previous_final_update_id,
                side: "BID".to_string(),
                price,
                quantity,
            });
        }
        for (price_str, qty_str) in &event.asks {
            let (Ok(price), Ok(quantity)) = (
                price_str_to_ticks(price_str),
                quantity_str_to_ticks(qty_str),
            ) else {
                continue;
            };
            rows.push(LevelChangeRow {
                session_id: self.session.session_id,
                seq,
                symbol: symbol.clone(),
                exchange_event_time,
                exchange_transaction_time,
                local_receive_time,
                first_update_id: event.first_update_id,
                final_update_id: event.final_update_id,
                previous_final_update_id: event.previous_final_update_id,
                side: "ASK".to_string(),
                price,
                quantity,
            });
        }

        if rows.is_empty() {
            return;
        }
        if !self.send(Record::Depth { rows }) {
            self.metrics.lock().unwrap().depth_dropped += 1;
        }
    }

    /// Record an order-book snapshot (initial sync or resync).
    pub fn record_snapshot(
        &self,
        symbol: &str,
        snapshot_update_id: u64,
        timestamp_ms: u64,
        bids: &[(String, String)],
        asks: &[(String, String)],
    ) {
        let row = SnapshotRow {
            session_id: self.session.session_id,
            seq: self.next_seq(),
            symbol: symbol.to_string(),
            snapshot_update_id,
            timestamp: ms_to_datetime(timestamp_ms),
            bids: encode_levels(bids),
            asks: encode_levels(asks),
        };
        let _ = self.send(Record::Snapshot(row));
    }

    /// Record that a marker event was rejected (never becomes a trade row).
    pub fn record_marker_rejected(&self) {
        self.metrics.lock().unwrap().marker_rejected += 1;
    }

    /// Record that an invalid event was rejected (never becomes a row).
    pub fn record_invalid_rejected(&self) {
        self.metrics.lock().unwrap().invalid_rejected += 1;
    }

    fn send(&self, record: Record) -> bool {
        match self.tx.try_send(record) {
            Ok(()) => {
                let depth = self
                    .queue_len
                    .fetch_add(1, Ordering::SeqCst)
                    .saturating_add(1);
                let mut m = self.metrics.lock().unwrap();
                if depth > m.queue_high_water {
                    m.queue_high_water = depth;
                }
                m.queue_depth = depth;
                true
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.on_queue_overflow();
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    }

    fn on_queue_overflow(&self) {
        let mut h = self.health.lock().unwrap();
        h.queue_overflows += 1;
        h.degraded = true;
        h.state = StorageState::Degraded;
        h.last_error = Some(format!(
            "storage queue full (capacity {}) — event dropped",
            self.metrics.lock().unwrap().queue_capacity
        ));
        error!(
            "CRITICAL STORAGE DIAGNOSTIC: recording queue full (capacity {}) — event dropped. \
             Entering DEGRADED state.",
            self.metrics.lock().unwrap().queue_capacity
        );
    }

    /// Current queue depth.
    pub fn queue_depth(&self) -> usize {
        self.queue_len.load(Ordering::SeqCst)
    }

    /// Session id for convenience.
    pub fn session_id(&self) -> Uuid {
        self.session.session_id
    }

    /// Signal the storage worker to shut down (drain + final flush).
    pub fn request_shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

fn encode_levels(levels: &[(String, String)]) -> String {
    let pairs: Vec<(u64, u64)> = levels
        .iter()
        .filter_map(|(p, q)| {
            let p = price_str_to_ticks(p).ok()?;
            let q = quantity_str_to_ticks(q).ok()?;
            Some((p, q))
        })
        .collect();
    serde_json::to_string(&pairs).unwrap_or_else(|_| "[]".to_string())
}

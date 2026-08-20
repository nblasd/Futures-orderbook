//! Storage worker: drains the bounded queue and batches inserts into
//! ClickHouse. Never blocks the market-data event loop.

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tracing::{error, warn};
use uuid::Uuid;

use crate::recording::metrics::{RecorderMetrics, StorageHealth, StorageState};
use crate::storage::{LevelChangeRow, RawEventRow, SnapshotRow, Storage, TradeRow};

/// A single record queued for storage.
#[derive(Debug)]
pub enum Record {
    Raw(RawEventRow),
    Trade(TradeRow),
    Depth { rows: Vec<LevelChangeRow> },
    Snapshot(SnapshotRow),
}

/// Tuning knobs for the storage worker.
#[derive(Debug, Clone)]
pub struct RecordingConfig {
    /// Rows per type that trigger a flush.
    pub batch_size: usize,
    /// Max time between flushes.
    pub flush_interval: Duration,
    /// Bounded channel capacity.
    pub queue_capacity: usize,
    /// Insert attempts per batch before giving up (batch is retained).
    pub retry_attempts: u32,
    /// Base backoff between retries.
    pub retry_backoff: Duration,
}

impl RecordingConfig {
    pub fn new(batch_size: usize, flush_interval_ms: u64, queue_capacity: usize) -> Self {
        Self {
            batch_size: batch_size.max(1),
            flush_interval: Duration::from_millis(flush_interval_ms.max(1)),
            queue_capacity: queue_capacity.max(1),
            retry_attempts: 5,
            retry_backoff: Duration::from_millis(200),
        }
    }
}

impl Default for RecordingConfig {
    fn default() -> Self {
        Self::new(1000, 250, 100_000)
    }
}

/// The storage worker task. Runs until the channel closes, then performs a
/// final flush of all pending data.
pub struct StorageWorker {
    rx: mpsc::Receiver<Record>,
    storage: Arc<dyn Storage>,
    config: RecordingConfig,
    session_id: Uuid,
    metrics: Arc<Mutex<RecorderMetrics>>,
    health: Arc<Mutex<StorageHealth>>,
    queue_len: Arc<AtomicUsize>,
    // Pending batches (per type). Failed batches are retained and retried.
    pending_raw: Vec<RawEventRow>,
    pending_trades: Vec<TradeRow>,
    pending_depth: Vec<LevelChangeRow>,
    pending_snapshot: Option<SnapshotRow>,
    last_synced_degraded: bool,
}

impl StorageWorker {
    pub fn new(
        rx: mpsc::Receiver<Record>,
        storage: Arc<dyn Storage>,
        config: RecordingConfig,
        session_id: Uuid,
        metrics: Arc<Mutex<RecorderMetrics>>,
        health: Arc<Mutex<StorageHealth>>,
        queue_len: Arc<AtomicUsize>,
    ) -> Self {
        metrics.lock().unwrap().queue_capacity = config.queue_capacity;
        Self {
            rx,
            storage,
            config,
            session_id,
            metrics,
            health,
            queue_len,
            pending_raw: Vec::new(),
            pending_trades: Vec::new(),
            pending_depth: Vec::new(),
            pending_snapshot: None,
            last_synced_degraded: false,
        }
    }

    pub fn config(&self) -> &RecordingConfig {
        &self.config
    }

    pub fn recv(&mut self) -> impl std::future::Future<Output = Option<Record>> + Send + '_ {
        self.rx.recv()
    }

    pub fn recv_try(&mut self) -> Result<Record, mpsc::error::TryRecvError> {
        self.rx.try_recv()
    }

    pub fn enqueue(&mut self, record: Record) {
        self.queue_len.fetch_sub(1, Ordering::SeqCst);
        match record {
            Record::Raw(row) => self.pending_raw.push(row),
            Record::Trade(row) => self.pending_trades.push(row),
            Record::Depth { rows } => self.pending_depth.extend(rows),
            Record::Snapshot(row) => self.pending_snapshot = Some(row),
        }
    }

    pub fn threshold_reached(&self) -> bool {
        self.pending_raw.len() >= self.config.batch_size
            || self.pending_trades.len() >= self.config.batch_size
            || self.pending_depth.len() >= self.config.batch_size
    }

    pub async fn flush_all(&mut self) {
        self.flush_raw().await;
        self.flush_trades().await;
        self.flush_depth().await;
        self.flush_snapshot().await;
        self.sync_session_health().await;
    }

    async fn flush_raw(&mut self) {
        if self.pending_raw.is_empty() {
            return;
        }
        let start = Instant::now();
        let len = self.pending_raw.len() as u64;
        let bytes: u64 = self
            .pending_raw
            .iter()
            .map(|r| r.raw_payload.len() as u64)
            .sum();
        let batch = std::mem::take(&mut self.pending_raw);
        let (result, returned) = retry_insert(
            self.storage.as_ref(),
            &self.config,
            batch,
            Arc::clone(&self.metrics),
            |s, b| Box::pin(async move { s.insert_raw_events(&b).await.map_err(|e| (e, b)) }),
        )
        .await;
        match result {
            Ok(()) => {
                self.metrics
                    .lock()
                    .unwrap()
                    .record_flush(start.elapsed().as_nanos());
                let mut m = self.metrics.lock().unwrap();
                m.batches_flushed += 1;
                m.raw_stored += len;
                m.raw_bytes_stored += bytes;
            }
            Err(e) => {
                self.pending_raw.extend(returned);
                self.metrics.lock().unwrap().failed_batches += 1;
                self.mark_degraded(&format!("raw insert failed: {}", e));
            }
        }
    }

    async fn flush_trades(&mut self) {
        if self.pending_trades.is_empty() {
            return;
        }
        let start = Instant::now();
        let len = self.pending_trades.len() as u64;
        let batch = std::mem::take(&mut self.pending_trades);
        let (result, returned) = retry_insert(
            self.storage.as_ref(),
            &self.config,
            batch,
            Arc::clone(&self.metrics),
            |s, b| Box::pin(async move { s.insert_trades(&b).await.map_err(|e| (e, b)) }),
        )
        .await;
        match result {
            Ok(()) => {
                self.metrics
                    .lock()
                    .unwrap()
                    .record_flush(start.elapsed().as_nanos());
                let mut m = self.metrics.lock().unwrap();
                m.batches_flushed += 1;
                m.trades_stored += len;
            }
            Err(e) => {
                self.pending_trades.extend(returned);
                self.metrics.lock().unwrap().failed_batches += 1;
                self.mark_degraded(&format!("trades insert failed: {}", e));
            }
        }
    }

    async fn flush_depth(&mut self) {
        if self.pending_depth.is_empty() {
            return;
        }
        let start = Instant::now();
        let len = self.pending_depth.len() as u64;
        let batch = std::mem::take(&mut self.pending_depth);
        let (result, returned) = retry_insert(
            self.storage.as_ref(),
            &self.config,
            batch,
            Arc::clone(&self.metrics),
            |s, b| Box::pin(async move { s.insert_level_changes(&b).await.map_err(|e| (e, b)) }),
        )
        .await;
        match result {
            Ok(()) => {
                self.metrics
                    .lock()
                    .unwrap()
                    .record_flush(start.elapsed().as_nanos());
                let mut m = self.metrics.lock().unwrap();
                m.batches_flushed += 1;
                m.level_changes_stored += len;
            }
            Err(e) => {
                self.pending_depth.extend(returned);
                self.metrics.lock().unwrap().failed_batches += 1;
                self.mark_degraded(&format!("depth insert failed: {}", e));
            }
        }
    }

    async fn flush_snapshot(&mut self) {
        let Some(snapshot) = self.pending_snapshot.take() else {
            return;
        };
        let start = Instant::now();
        let (result, returned) = retry_insert(
            self.storage.as_ref(),
            &self.config,
            vec![snapshot],
            Arc::clone(&self.metrics),
            |s, b| {
                let snap = b.into_iter().next().unwrap();
                Box::pin(async move { s.insert_snapshot(&snap).await.map_err(|e| (e, vec![snap])) })
            },
        )
        .await;
        match result {
            Ok(()) => {
                self.metrics
                    .lock()
                    .unwrap()
                    .record_flush(start.elapsed().as_nanos());
                let mut m = self.metrics.lock().unwrap();
                m.batches_flushed += 1;
                m.snapshots_stored += 1;
            }
            Err(e) => {
                self.pending_snapshot = returned.into_iter().next();
                self.metrics.lock().unwrap().failed_batches += 1;
                self.mark_degraded(&format!("snapshot insert failed: {}", e));
            }
        }
    }

    /// Persist the DEGRADED/RECORDING health state into the session row.
    async fn sync_session_health(&mut self) {
        let degraded = self.health.lock().unwrap().degraded;
        if degraded == self.last_synced_degraded {
            return;
        }
        let status = if degraded { "DEGRADED" } else { "RECORDING" };
        if let Err(e) = self
            .storage
            .update_session_status(self.session_id, status, None)
            .await
        {
            warn!("Failed to update session status to {}: {}", status, e);
        } else {
            self.last_synced_degraded = degraded;
        }
    }

    fn mark_degraded(&mut self, msg: &str) {
        error!("CRITICAL STORAGE DIAGNOSTIC: {}", msg);
        let mut h = self.health.lock().unwrap();
        h.degraded = true;
        h.insert_failures += 1;
        h.last_error = Some(msg.to_string());
        h.state = StorageState::Degraded;
    }
}

/// Insert a batch with bounded retries. On failure the batch is returned to
/// the caller so it is never silently dropped. Because every row carries a
/// deterministic identity and the tables are `ReplacingMergeTree`, retrying an
/// identical batch is idempotent (duplicate rows collapse on merge).
async fn retry_insert<T: Send + 'static>(
    storage: &dyn Storage,
    config: &RecordingConfig,
    batch: Vec<T>,
    metrics: Arc<Mutex<RecorderMetrics>>,
    mut insert: impl for<'a> FnMut(
        &'a dyn Storage,
        Vec<T>,
    ) -> Pin<
        Box<dyn std::future::Future<Output = Result<(), (anyhow::Error, Vec<T>)>> + Send + 'a>,
    >,
) -> (anyhow::Result<()>, Vec<T>) {
    let mut batch = batch;
    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 0..config.retry_attempts {
        match insert(storage, batch).await {
            Ok(()) => return (Ok(()), Vec::new()),
            Err((e, returned)) => {
                last_err = Some(e);
                batch = returned;
                metrics.lock().unwrap().retries += 1;
                warn!(
                    "Storage insert failed (attempt {}/{}): {}",
                    attempt + 1,
                    config.retry_attempts,
                    last_err.as_ref().unwrap()
                );
                tokio::time::sleep(config.retry_backoff * (attempt + 1)).await;
            }
        }
    }
    (
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("storage insert failed"))),
        batch,
    )
}

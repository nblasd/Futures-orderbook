//! Deterministic replay engine.
//!
//! Feeds the reconstructed event stream through the **same** components the
//! live engine uses — [`Synchronizer`], [`OrderBook`] and [`TradeProcessor`] —
//! so the replayed book state matches the recorded one. Read-only; never
//! touches the network.

use crate::orderbook::book::OrderBook;
use crate::orderbook::synchronizer::{ProcessResult, SyncState, Synchronizer};
use crate::replay::reader::SessionData;
use crate::replay::timing::ReplayTiming;
use crate::trades::processor::{TradeProcessResult, TradeProcessor};

#[derive(Debug, Clone)]
pub struct ReplayConfig {
    /// 0 = as fast as possible; 1 = real-time; >1 = accelerated.
    pub speed: f64,
    /// If set, only replay events with `seq` in `[start, end]`.
    pub seq_start: Option<u64>,
    pub seq_end: Option<u64>,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            speed: 0.0,
            seq_start: None,
            seq_end: None,
        }
    }
}

/// Final state and counters produced by a replay run.
#[derive(Debug, Clone, Default)]
pub struct ReplayOutcome {
    pub events_total: u64,
    pub depth_events: u64,
    pub snapshots_applied: u64,
    pub events_applied: u64,
    pub events_ignored: u64,
    pub sequence_errors: u64,
    pub trades_processed: u64,
    pub trades_skipped: u64,
    pub final_update_id: u64,
    pub book_bid_levels: usize,
    pub book_ask_levels: usize,
    pub best_bid: Option<u64>,
    pub best_ask: Option<u64>,
    pub mid_price: Option<f64>,
    pub spread: Option<u64>,
    pub duration_ns: u128,
}

/// Run a replay over the given session data.
pub async fn run_replay(data: SessionData, config: ReplayConfig) -> anyhow::Result<ReplayOutcome> {
    let mut sync = Synchronizer::new();
    let mut book = OrderBook::new();
    let mut trade_proc = TradeProcessor::new();

    // Mirror live: once the depth stream connects we begin buffering.
    sync.on_connected();

    let mut outcome = ReplayOutcome {
        events_total: data.events.len() as u64,
        ..ReplayOutcome::default()
    };

    let mut timing = ReplayTiming::new(config.speed);
    let started = std::time::Instant::now();

    for event in &data.events {
        if let Some(start) = config.seq_start {
            if event.seq() < start {
                continue;
            }
        }
        if let Some(end) = config.seq_end {
            if event.seq() > end {
                break;
            }
        }

        timing.pace(event.time_ms()).await;

        match event {
            crate::replay::reader::ReplayEvent::Snapshot {
                update_id,
                bids,
                asks,
                ..
            } => {
                outcome.snapshots_applied += 1;
                sync.on_snapshot_loading();
                book.apply_snapshot(bids, asks, *update_id)?;
                match sync.reconcile(*update_id) {
                    Ok(events) => {
                        for e in &events {
                            book.apply_depth_update(&e.bids, &e.asks, e.final_update_id)?;
                            outcome.events_applied += 1;
                        }
                    }
                    Err(_) => {
                        // Same behaviour as live: no bridging event → resync.
                        outcome.sequence_errors += 1;
                        sync.trigger_resync();
                    }
                }
            }
            crate::replay::reader::ReplayEvent::Depth { update, .. } => {
                outcome.depth_events += 1;
                match sync.state() {
                    SyncState::Buffering
                    | SyncState::Reconnecting
                    | SyncState::SnapshotLoading
                    | SyncState::Synchronizing => {
                        sync.buffer_event(update.clone());
                    }
                    SyncState::Ready => match sync.process_live_event(update) {
                        ProcessResult::Apply => {
                            book.apply_depth_update(
                                &update.bids,
                                &update.asks,
                                update.final_update_id,
                            )?;
                            outcome.events_applied += 1;
                        }
                        ProcessResult::Stale | ProcessResult::Ignored => {
                            outcome.events_ignored += 1;
                        }
                        ProcessResult::PuMismatch => {
                            outcome.sequence_errors += 1;
                            sync.trigger_resync();
                        }
                        ProcessResult::Buffered => {}
                    },
                    _ => {
                        outcome.events_ignored += 1;
                    }
                }
            }
            crate::replay::reader::ReplayEvent::Trade { event, .. } => {
                match trade_proc.process(event.clone()) {
                    TradeProcessResult::Processed => outcome.trades_processed += 1,
                    _ => outcome.trades_skipped += 1,
                }
            }
        }
    }

    outcome.duration_ns = started.elapsed().as_nanos();
    outcome.final_update_id = book.last_update_id();
    let snap = book.snapshot();
    outcome.book_bid_levels = snap.bids.len();
    outcome.book_ask_levels = snap.asks.len();
    outcome.best_bid = snap.best_bid;
    outcome.best_ask = snap.best_ask;
    outcome.mid_price = snap.mid_price;
    outcome.spread = snap.best_bid.and_then(|b| snap.best_ask.map(|a| a - b));

    Ok(outcome)
}

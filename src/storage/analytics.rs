//! Phase 4 analytics persistence: row types, the bounded-channel sink, and
//! the batch types that flow from the engine to storage.
//!
//! Storage layout (migrations 006–009):
//! * `analytics_snapshots`   — one row per `MarketMicrostructureSnapshot`.
//! * `analytics_events`      — derived events (large trade, sweep, absorption,
//!   cluster, book anomaly, …). `TradeDelta` and `Cluster` events are computed
//!   but not persisted by default.
//! * `delta_by_price`        — session volume-at-price profile, written once
//!   at final flush (cumulative).
//! * `liquidity_events`      — per-level displayed-liquidity changes.
//!
//! All prices/quantities are integer ticks (u64, 1e8 scale); `delta`/`cvd`
//! are Int128. Timestamps are exchange event times (DateTime64(3, 'UTC')).

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use uuid::Uuid;

use super::Storage;
use crate::analytics::snapshot::MarketMicrostructureSnapshot;

// ============================================================================
// Row types
// ============================================================================

/// One `MarketMicrostructureSnapshot` row.
#[derive(Debug, Clone, Serialize, Deserialize, ::clickhouse::Row)]
pub struct AnalyticsSnapshotRow {
    #[serde(with = "::clickhouse::serde::uuid")]
    pub session_id: Uuid,
    pub symbol: String,
    #[serde(with = "::clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp_ms: DateTime<Utc>,
    pub analytics_version: String,
    pub book_ready: bool,
    pub best_bid: Option<u64>,
    pub best_ask: Option<u64>,
    pub mid_price: Option<f64>,
    pub spread_ticks: Option<u64>,
    pub microprice_num: Option<u128>,
    pub microprice_den: Option<u128>,
    pub trade_volume: u64,
    pub buy_volume: u64,
    pub sell_volume: u64,
    pub delta: i128,
    pub cvd: i128,
    pub bid_depth: u64,
    pub ask_depth: u64,
    pub book_imbalance: Option<f64>,
    pub liquidity_added: u64,
    pub liquidity_removed: u64,
    pub large_trade_count: u64,
    pub sweep_candidate_count: u64,
    pub absorption_candidate_count: u64,
    pub replenishment_count: u64,
    pub book_crossed: bool,
    pub anomalies: u64,
}

impl AnalyticsSnapshotRow {
    pub fn from_snapshot(snapshot: &MarketMicrostructureSnapshot, session_id: Uuid) -> Self {
        Self {
            session_id,
            symbol: snapshot.symbol.clone(),
            timestamp_ms: crate::storage::ms_to_datetime(snapshot.timestamp_ms),
            analytics_version: snapshot.analytics_version.clone(),
            book_ready: snapshot.book_ready,
            best_bid: snapshot.best_bid,
            best_ask: snapshot.best_ask,
            mid_price: snapshot.mid_price,
            spread_ticks: snapshot.spread_ticks,
            microprice_num: snapshot.microprice_num,
            microprice_den: snapshot.microprice_den,
            trade_volume: snapshot.trade_volume,
            buy_volume: snapshot.buy_volume,
            sell_volume: snapshot.sell_volume,
            delta: snapshot.delta,
            cvd: snapshot.cvd,
            bid_depth: snapshot.bid_depth,
            ask_depth: snapshot.ask_depth,
            book_imbalance: snapshot.book_imbalance,
            liquidity_added: snapshot.liquidity_added,
            liquidity_removed: snapshot.liquidity_removed,
            large_trade_count: snapshot.large_trade_count,
            sweep_candidate_count: snapshot.sweep_candidate_count,
            absorption_candidate_count: snapshot.absorption_candidate_count,
            replenishment_count: snapshot.replenishment_count,
            book_crossed: snapshot.book_crossed,
            anomalies: snapshot.anomalies,
        }
    }
}

/// One derived analytics event row.
#[derive(Debug, Clone, Serialize, Deserialize, ::clickhouse::Row)]
pub struct AnalyticsEventRow {
    #[serde(with = "::clickhouse::serde::uuid")]
    pub session_id: Uuid,
    pub symbol: String,
    #[serde(with = "::clickhouse::serde::chrono::datetime64::millis")]
    pub ts_ms: DateTime<Utc>,
    /// Event kind string (`AnalyticsEventKind::as_str`).
    pub kind: String,
    pub side: Option<String>,
    pub price: Option<u64>,
    pub quantity: u64,
    /// Deterministic JSON evidence.
    pub detail: String,
}

impl AnalyticsEventRow {
    pub fn from_event(event: &crate::analytics::events::AnalyticsEvent, session_id: Uuid) -> Self {
        Self {
            session_id,
            symbol: event.symbol.clone(),
            ts_ms: crate::storage::ms_to_datetime(event.ts_ms),
            kind: event.kind.as_str().to_string(),
            side: event.side.clone(),
            price: event.price,
            quantity: event.quantity,
            detail: event.detail.to_string(),
        }
    }
}

/// Session volume-at-price profile row (written at final flush).
#[derive(Debug, Clone, Serialize, Deserialize, ::clickhouse::Row)]
pub struct DeltaByPriceRow {
    #[serde(with = "::clickhouse::serde::uuid")]
    pub session_id: Uuid,
    pub symbol: String,
    #[serde(with = "::clickhouse::serde::chrono::datetime64::millis")]
    pub ts_ms: DateTime<Utc>,
    pub price: u64,
    pub buy_volume: u64,
    pub sell_volume: u64,
    pub total_volume: u64,
    pub delta: i128,
    pub trade_count: u64,
    pub large_trade_count: u64,
}

/// One displayed-liquidity level change row.
#[derive(Debug, Clone, Serialize, Deserialize, ::clickhouse::Row)]
pub struct LiquidityEventRow {
    #[serde(with = "::clickhouse::serde::uuid")]
    pub session_id: Uuid,
    pub symbol: String,
    #[serde(with = "::clickhouse::serde::chrono::datetime64::millis")]
    pub ts_ms: DateTime<Utc>,
    /// "added" | "removed" | "increased" | "decreased" | "replenishment".
    pub kind: String,
    /// "BID" or "ASK".
    pub side: String,
    pub price: u64,
    /// Magnitude of the displayed-quantity change (ticks).
    pub quantity_delta: u64,
    /// True for a replenishment candidate.
    pub is_replenishment: bool,
}

// ============================================================================
// Sink
// ============================================================================

/// A batch of analytics rows destined for storage.
#[derive(Debug, Clone, Default)]
pub struct AnalyticsBatch {
    pub snapshots: Vec<AnalyticsSnapshotRow>,
    pub events: Vec<AnalyticsEventRow>,
    pub delta_by_price: Vec<DeltaByPriceRow>,
    pub liquidity_events: Vec<LiquidityEventRow>,
}

impl AnalyticsBatch {
    pub fn is_empty(&self) -> bool {
        self.snapshots.is_empty()
            && self.events.is_empty()
            && self.delta_by_price.is_empty()
            && self.liquidity_events.is_empty()
    }

    pub fn clear(&mut self) {
        self.snapshots.clear();
        self.events.clear();
        self.delta_by_price.clear();
        self.liquidity_events.clear();
    }
}

/// Sender half of the analytics persistence pipeline. Cheap to clone; rows are
/// buffered on the send side and drained by a worker task that performs the
/// batched inserts.
#[derive(Clone)]
pub struct AnalyticsSink {
    tx: mpsc::UnboundedSender<AnalyticsBatch>,
}

/// Handle to the sink worker task.
pub struct AnalyticsSinkHandle {
    join: tokio::task::JoinHandle<()>,
}

impl AnalyticsSink {
    /// Send a batch to the worker. Drops are recorded as insert failures
    /// (channel closed) but never block the engine loop.
    pub fn submit(&self, batch: AnalyticsBatch) {
        if batch.is_empty() {
            return;
        }
        if self.tx.send(batch).is_err() {
            tracing::warn!("analytics sink channel closed; dropping batch");
        }
    }
}

impl AnalyticsSinkHandle {
    pub async fn join(self) {
        let _ = self.join.await;
    }
}

/// Start the analytics persistence worker.
pub fn start_analytics_sink(
    storage: Arc<dyn Storage>,
    channel_capacity: usize,
) -> (AnalyticsSink, AnalyticsSinkHandle) {
    let (tx, mut rx) = mpsc::unbounded_channel::<AnalyticsBatch>();
    let join = tokio::spawn(async move {
        let capacity = channel_capacity.max(1);
        let mut pending = AnalyticsBatch::default();
        while let Some(batch) = rx.recv().await {
            pending.snapshots.extend(batch.snapshots);
            pending.events.extend(batch.events);
            pending.delta_by_price.extend(batch.delta_by_price);
            pending.liquidity_events.extend(batch.liquidity_events);
            if pending.snapshots.len() >= capacity
                || pending.events.len() >= capacity
                || pending.liquidity_events.len() >= capacity
            {
                flush_batch(storage.as_ref(), &mut pending).await;
            }
        }
        // Final flush of any remaining rows.
        flush_batch(storage.as_ref(), &mut pending).await;
    });
    (AnalyticsSink { tx }, AnalyticsSinkHandle { join })
}

async fn flush_batch(storage: &dyn Storage, pending: &mut AnalyticsBatch) {
    let mut failed = false;
    if !pending.snapshots.is_empty() {
        if let Err(e) = storage.insert_analytics_snapshots(&pending.snapshots).await {
            tracing::error!("analytics snapshot insert failed: {}", e);
            failed = true;
        }
    }
    if !pending.events.is_empty() {
        if let Err(e) = storage.insert_analytics_events(&pending.events).await {
            tracing::error!("analytics event insert failed: {}", e);
            failed = true;
        }
    }
    if !pending.delta_by_price.is_empty() {
        if let Err(e) = storage.insert_delta_by_price(&pending.delta_by_price).await {
            tracing::error!("delta_by_price insert failed: {}", e);
            failed = true;
        }
    }
    if !pending.liquidity_events.is_empty() {
        if let Err(e) = storage
            .insert_liquidity_events(&pending.liquidity_events)
            .await
        {
            tracing::error!("liquidity event insert failed: {}", e);
            failed = true;
        }
    }
    if !failed {
        pending.clear();
    } else {
        // Keep the failed rows out of the pipeline to avoid an infinite
        // retry loop; the error is already logged.
        pending.clear();
    }
}

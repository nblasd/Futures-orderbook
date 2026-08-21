//! Persistent storage layer for Phase 3 market-data recording.
//!
//! A `Storage` trait abstracts over the two concrete backends:
//! - [`::clickhouse::ClickHouseStorage`] — the production backend.
//! - [`memory::MemoryStorage`] — an in-process backend used by the offline
//!   end-to-end tests (no network, no ClickHouse required).

pub mod analytics;
pub mod ch;
pub mod memory;
pub mod migrations;

pub use analytics::{
    start_analytics_sink, AnalyticsBatch, AnalyticsEventRow, AnalyticsSink, AnalyticsSinkHandle,
    AnalyticsSnapshotRow, DeltaByPriceRow, LiquidityEventRow,
};
pub use ch::ClickHouseStorage;
pub use memory::{FlakyStorage, MemoryStorage};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::recording::session::SessionRecord;

// ============================================================================
// Time helpers
// ============================================================================

/// Convert exchange millisecond timestamps to a UTC `DateTime`.
pub fn ms_to_datetime(ms: u64) -> DateTime<Utc> {
    let secs = (ms / 1000) as i64;
    let nanos = (ms % 1000) as u32 * 1_000_000;
    DateTime::from_timestamp(secs, nanos).unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap())
}

/// Convert a nanosecond `u128` timestamp (as used by `local_receive_time_ns`)
/// to a UTC `DateTime`.
pub fn ns_to_datetime(ns: u128) -> DateTime<Utc> {
    let secs = (ns / 1_000_000_000) as i64;
    let nanos = (ns % 1_000_000_000) as u32;
    DateTime::from_timestamp(secs, nanos).unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap())
}

/// Convert a UTC `DateTime` back to nanoseconds since the Unix epoch.
pub fn datetime_to_ns(dt: DateTime<Utc>) -> u128 {
    let secs = dt.timestamp() as i128;
    let nanos = dt.timestamp_subsec_nanos() as i128;
    (secs * 1_000_000_000 + nanos) as u128
}

/// Convert a UTC `DateTime` back to milliseconds since the Unix epoch.
pub fn datetime_to_ms(dt: DateTime<Utc>) -> u64 {
    (dt.timestamp() as u64) * 1000 + (dt.timestamp_subsec_millis() as u64)
}

// ============================================================================
// Row types
// ============================================================================

/// A single raw Binance WebSocket payload, preserved byte-for-byte.
#[derive(Debug, Clone, Serialize, Deserialize, ::clickhouse::Row)]
pub struct RawEventRow {
    #[serde(with = "::clickhouse::serde::uuid")]
    pub session_id: Uuid,
    /// Global monotonic sequence assigned by the recorder. Deterministic
    /// ordering key for replay.
    pub seq: u64,
    pub symbol: String,
    /// "depth" or "trade".
    pub stream_type: String,
    /// Exchange event time (`E`).
    #[serde(with = "::clickhouse::serde::chrono::datetime64::millis")]
    pub exchange_event_time: DateTime<Utc>,
    /// Exchange transaction time (`T`) when available.
    #[serde(with = "::clickhouse::serde::chrono::datetime64::millis::option")]
    pub exchange_transaction_time: Option<DateTime<Utc>>,
    /// Local receive time (nanosecond precision).
    #[serde(with = "::clickhouse::serde::chrono::datetime64::nanos")]
    pub local_receive_time: DateTime<Utc>,
    /// The exact JSON payload as received. Never mutated or pretty-printed.
    pub raw_payload: String,
}

/// A normalized trade row.
#[derive(Debug, Clone, Serialize, Deserialize, ::clickhouse::Row)]
pub struct TradeRow {
    #[serde(with = "::clickhouse::serde::uuid")]
    pub session_id: Uuid,
    pub seq: u64,
    pub symbol: String,
    pub trade_id: u64,
    pub first_trade_id: Option<u64>,
    pub last_trade_id: Option<u64>,
    /// Price in integer ticks (u64, 1e8 scale). Exact representation.
    pub price: u64,
    /// Quantity in integer ticks (u64, 1e8 scale). Exact representation.
    pub quantity: u64,
    /// "BUY" or "SELL".
    pub aggressor_side: String,
    #[serde(with = "::clickhouse::serde::chrono::datetime64::millis")]
    pub exchange_event_time: DateTime<Utc>,
    #[serde(with = "::clickhouse::serde::chrono::datetime64::millis")]
    pub trade_time: DateTime<Utc>,
    #[serde(with = "::clickhouse::serde::chrono::datetime64::nanos")]
    pub local_receive_time: DateTime<Utc>,
    pub order_type: String,
}

/// A single price-level change within a depth event.
///
/// Quantity 0 means the level was removed. This is one row of a depth event;
/// all rows sharing the same `(session_id, seq)` belong to the same depth
/// event and can be grouped back into a `DepthUpdate` for replay.
#[derive(Debug, Clone, Serialize, Deserialize, ::clickhouse::Row)]
pub struct LevelChangeRow {
    #[serde(with = "::clickhouse::serde::uuid")]
    pub session_id: Uuid,
    pub seq: u64,
    pub symbol: String,
    #[serde(with = "::clickhouse::serde::chrono::datetime64::millis")]
    pub exchange_event_time: DateTime<Utc>,
    #[serde(with = "::clickhouse::serde::chrono::datetime64::millis")]
    pub exchange_transaction_time: DateTime<Utc>,
    #[serde(with = "::clickhouse::serde::chrono::datetime64::nanos")]
    pub local_receive_time: DateTime<Utc>,
    /// Binance `U`.
    pub first_update_id: u64,
    /// Binance `u`.
    pub final_update_id: u64,
    /// Binance `pu`.
    pub previous_final_update_id: u64,
    /// "BID" or "ASK".
    pub side: String,
    /// Price in integer ticks.
    pub price: u64,
    /// New absolute quantity in integer ticks (0 = removal).
    pub quantity: u64,
}

/// An order-book snapshot (initial synchronization or resync).
///
/// `bids`/`asks` are stored as JSON arrays of `[price_ticks, qty_ticks]`
/// pairs, sufficient to reconstruct the initial book for replay.
#[derive(Debug, Clone, Serialize, Deserialize, ::clickhouse::Row)]
pub struct SnapshotRow {
    #[serde(with = "::clickhouse::serde::uuid")]
    pub session_id: Uuid,
    pub seq: u64,
    pub symbol: String,
    pub snapshot_update_id: u64,
    #[serde(with = "::clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
    pub bids: String,
    pub asks: String,
}

// ============================================================================
// Storage trait
// ============================================================================

/// Read/write access to persistent market-data storage.
///
/// Implementations are required to be safe to share across tasks.
#[async_trait::async_trait]
pub trait Storage: Send + Sync {
    /// Check connectivity (and, for ClickHouse, that the schema exists).
    async fn ping(&self) -> anyhow::Result<()>;

    /// Initialize the schema (create tables, apply migrations). Idempotent.
    async fn init_schema(&self) -> anyhow::Result<()>;

    /// Insert a new session record.
    async fn insert_session(&self, session: &SessionRecord) -> anyhow::Result<()>;

    /// Update a session's status and optional end time.
    async fn update_session_status(
        &self,
        session_id: Uuid,
        status: &str,
        ended_at: Option<DateTime<Utc>>,
    ) -> anyhow::Result<()>;

    /// Batch-insert raw events.
    async fn insert_raw_events(&self, batch: &[RawEventRow]) -> anyhow::Result<()>;

    /// Batch-insert normalized trades.
    async fn insert_trades(&self, batch: &[TradeRow]) -> anyhow::Result<()>;

    /// Batch-insert order-book level changes.
    async fn insert_level_changes(&self, batch: &[LevelChangeRow]) -> anyhow::Result<()>;

    /// Insert an order-book snapshot.
    async fn insert_snapshot(&self, snapshot: &SnapshotRow) -> anyhow::Result<()>;

    /// Look up a session by id.
    async fn get_session(&self, session_id: Uuid) -> anyhow::Result<Option<SessionRecord>>;

    /// List the most recent sessions (ordered by start time, descending).
    async fn list_sessions(&self, limit: u64) -> anyhow::Result<Vec<SessionRecord>>;

    /// Read all snapshots for a session, ordered by seq.
    async fn read_snapshots(&self, session_id: Uuid) -> anyhow::Result<Vec<SnapshotRow>>;

    /// Read all level changes for a session, ordered by seq.
    async fn read_level_changes(&self, session_id: Uuid) -> anyhow::Result<Vec<LevelChangeRow>>;

    /// Read level changes for a symbol within a time range, ordered by seq.
    async fn read_level_changes_range(
        &self,
        symbol: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> anyhow::Result<Vec<LevelChangeRow>>;

    /// Read all trades for a session, ordered by seq.
    async fn read_trades(&self, session_id: Uuid) -> anyhow::Result<Vec<TradeRow>>;

    /// Read trades for a symbol within a time range, ordered by seq.
    async fn read_trades_range(
        &self,
        symbol: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> anyhow::Result<Vec<TradeRow>>;

    /// Read all raw events for a session, ordered by seq.
    async fn read_raw_events(&self, session_id: Uuid) -> anyhow::Result<Vec<RawEventRow>>;

    /// Read raw events for a symbol within a time range, ordered by seq.
    async fn read_raw_events_range(
        &self,
        symbol: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> anyhow::Result<Vec<RawEventRow>>;

    /// Number of trade rows for a session.
    async fn count_trades(&self, session_id: Uuid) -> anyhow::Result<u64>;

    /// Number of level-change rows for a session.
    async fn count_level_changes(&self, session_id: Uuid) -> anyhow::Result<u64>;

    /// Number of distinct depth events (distinct seq) for a session.
    async fn count_depth_events(&self, session_id: Uuid) -> anyhow::Result<u64>;

    /// Number of raw event rows for a session.
    async fn count_raw_events(&self, session_id: Uuid) -> anyhow::Result<u64>;

    /// Total on-disk size of the market-data database (bytes).
    async fn database_size(&self) -> anyhow::Result<u64>;

    // ------------------------------------------------------------------
    // Phase 4 analytics persistence
    // ------------------------------------------------------------------

    /// Batch-insert analytics snapshots.
    async fn insert_analytics_snapshots(
        &self,
        batch: &[AnalyticsSnapshotRow],
    ) -> anyhow::Result<()>;

    /// Batch-insert derived analytics events.
    async fn insert_analytics_events(&self, batch: &[AnalyticsEventRow]) -> anyhow::Result<()>;

    /// Batch-insert session volume-at-price profile rows.
    async fn insert_delta_by_price(&self, batch: &[DeltaByPriceRow]) -> anyhow::Result<()>;

    /// Batch-insert liquidity level-change rows.
    async fn insert_liquidity_events(&self, batch: &[LiquidityEventRow]) -> anyhow::Result<()>;

    /// Read analytics snapshots for a session, ordered by timestamp.
    async fn read_analytics_snapshots(
        &self,
        session_id: Uuid,
    ) -> anyhow::Result<Vec<AnalyticsSnapshotRow>>;

    /// Read analytics events for a session, ordered by ts.
    async fn read_analytics_events(
        &self,
        session_id: Uuid,
    ) -> anyhow::Result<Vec<AnalyticsEventRow>>;

    /// Read the delta-by-price profile for a session.
    async fn read_delta_by_price(&self, session_id: Uuid) -> anyhow::Result<Vec<DeltaByPriceRow>>;

    /// Read liquidity events for a session, ordered by ts.
    async fn read_liquidity_events(
        &self,
        session_id: Uuid,
    ) -> anyhow::Result<Vec<LiquidityEventRow>>;

    /// Number of analytics snapshot rows for a session.
    async fn count_analytics_snapshots(&self, session_id: Uuid) -> anyhow::Result<u64>;

    /// Number of analytics event rows for a session.
    async fn count_analytics_events(&self, session_id: Uuid) -> anyhow::Result<u64>;

    /// Downcast to the concrete type (for tests and diagnostics).
    fn as_any(&self) -> &dyn std::any::Any;
}

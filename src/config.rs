use clap::{Args, Parser, Subcommand};

use crate::analytics::config::{AnalyticsConfig, DEFAULT_ANALYTICS_VERSION};

/// Configuration for the futures order-book engine.
#[derive(Debug, Clone, Parser)]
#[command(
    name = "futures_orderbook",
    about = "Binance USDⓈ-M Futures BTCUSDT order-book engine"
)]
pub struct Config {
    /// Trading symbol (e.g., BTCUSDT)
    #[arg(long, default_value = "BTCUSDT")]
    pub symbol: String,

    /// REST base URL for Binance USDⓈ-M Futures
    #[arg(long, default_value = "https://fapi.binance.com")]
    pub rest_base: String,

    /// WebSocket base URL for Binance USDⓈ-M Futures
    #[arg(long, default_value = "wss://fstream.binance.com")]
    pub ws_base: String,

    /// Depth update speed (100ms or 250ms)
    #[arg(long, default_value = "100ms")]
    pub depth_speed: String,

    /// Depth limit for REST snapshot (5, 10, 20, 50, 100, 500, 1000)
    #[arg(long, default_value_t = 1000)]
    pub depth_limit: u32,

    /// Reconnect base delay in milliseconds
    #[arg(long, default_value_t = 1000)]
    pub reconnect_base_ms: u64,

    /// Reconnect maximum delay in milliseconds
    #[arg(long, default_value_t = 30_000)]
    pub reconnect_max_ms: u64,

    /// Diagnostic print interval in seconds
    #[arg(long, default_value_t = 2)]
    pub diagnostic_interval: u64,

    /// Run duration in seconds (0 = run indefinitely)
    #[arg(long, default_value_t = 0)]
    pub duration: u64,

    /// Enable market-data recording to ClickHouse.
    #[arg(long)]
    pub record: bool,

    /// ClickHouse HTTP URL.
    #[arg(long, env = "CLICKHOUSE_URL", default_value = "http://localhost:8123")]
    pub clickhouse_url: String,

    /// ClickHouse database name.
    #[arg(long, env = "CLICKHOUSE_DATABASE", default_value = "market_data")]
    pub clickhouse_database: String,

    /// ClickHouse user (empty = default).
    #[arg(long, env = "CLICKHOUSE_USER", default_value = "")]
    pub clickhouse_user: String,

    /// ClickHouse password (empty = none).
    #[arg(long, env = "CLICKHOUSE_PASSWORD", default_value = "")]
    pub clickhouse_password: String,

    /// Storage worker batch size (rows per type per flush).
    #[arg(long, env = "RECORDING_BATCH_SIZE", default_value_t = 1000)]
    pub batch_size: usize,

    /// Storage worker flush interval in milliseconds.
    #[arg(long, env = "RECORDING_FLUSH_INTERVAL", default_value_t = 250)]
    pub flush_interval_ms: u64,

    /// Storage queue capacity (bounded channel size).
    #[arg(long, env = "RECORDING_QUEUE_CAPACITY", default_value_t = 100_000)]
    pub queue_capacity: usize,

    // ------------------------------------------------------------------
    // Phase 4 analytics
    // ------------------------------------------------------------------
    /// Enable Phase 4 market-microstructure analytics (live and replay).
    #[arg(long)]
    pub analytics: bool,

    /// Absolute quantity threshold (BTC) for large-trade detection.
    #[arg(long, default_value_t = 5.0)]
    pub large_trade_btc: f64,

    /// Sweep detection window (ms).
    #[arg(long, default_value_t = 100)]
    pub sweep_window_ms: u64,

    /// Minimum distinct price levels for a sweep candidate.
    #[arg(long, default_value_t = 3)]
    pub sweep_min_levels: u32,

    /// Minimum aggregate volume (BTC) for a sweep candidate.
    #[arg(long, default_value_t = 5.0)]
    pub sweep_min_volume_btc: f64,

    /// Absorption detection window (ms).
    #[arg(long, default_value_t = 1000)]
    pub absorption_window_ms: u64,

    /// Minimum aggressive volume (BTC) for an absorption candidate.
    #[arg(long, default_value_t = 20.0)]
    pub absorption_min_volume_btc: f64,

    /// Minimum number of aggressive trades for an absorption candidate.
    #[arg(long, default_value_t = 5)]
    pub absorption_min_trades: u32,

    /// Maximum favorable price excursion (ticks) tolerated for absorption.
    #[arg(long, default_value_t = 3)]
    pub absorption_max_price_excursion_ticks: u64,

    /// Replenishment detection window (ms).
    #[arg(long, default_value_t = 250)]
    pub replenishment_window_ms: u64,

    /// Number of best levels scanned for book imbalance/depth.
    #[arg(long, default_value_t = 10)]
    pub imbalance_depth_levels: u32,

    /// Interval (ms) at which analytics snapshots are produced.
    #[arg(long, default_value_t = 1000)]
    pub analytics_snapshot_interval_ms: u64,

    /// In-memory analytics retention window (seconds).
    #[arg(long, default_value_t = 900)]
    pub analytics_retention_seconds: u64,

    /// Tick size for the symbol (e.g. "0.10" for BTCUSDT).
    #[arg(long, default_value = "0.10")]
    pub tick_size: String,

    /// Analytics algorithm version tag.
    #[arg(long, default_value = DEFAULT_ANALYTICS_VERSION)]
    pub analytics_version: String,

    /// Subcommand: replay or verify.
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Debug, Clone, Subcommand)]
pub enum Command {
    /// Replay a recorded session (or symbol/time range) through the same
    /// processing pipeline used by live ingestion. Read-only.
    Replay(ReplayArgs),
    /// Verify the integrity of a recorded session.
    Verify(VerifyArgs),
}

#[derive(Debug, Clone, Args)]
pub struct ReplayArgs {
    /// Session ID to replay.
    #[arg(long)]
    pub session: Option<String>,

    /// Symbol for time-range replay.
    #[arg(long)]
    pub symbol: Option<String>,

    /// Start time (RFC3339, UTC) for time-range replay.
    #[arg(long)]
    pub start: Option<String>,

    /// End time (RFC3339, UTC) for time-range replay.
    #[arg(long)]
    pub end: Option<String>,

    /// Replay speed: 1 = real-time, 10 = 10x, 0 = maximum speed.
    #[arg(long, default_value_t = 0.0)]
    pub speed: f64,

    /// Print diagnostics during replay.
    #[arg(long, default_value_t = false)]
    pub verbose: bool,

    /// Enable Phase 4 analytics during replay (computes a digest and compares
    /// it against the live digest for the same session, when available).
    #[arg(long, default_value_t = false)]
    pub analytics: bool,
}

#[derive(Debug, Clone, Args)]
pub struct VerifyArgs {
    /// Session ID to verify. If omitted, verify the most recent session.
    #[arg(long)]
    pub session: Option<String>,

    /// Also verify raw event counts against normalized counts.
    #[arg(long, default_value_t = false)]
    pub raw: bool,
}

impl Config {
    /// Build the WebSocket stream URL for the depth stream.
    ///
    /// Binance WebSocket stream names require lowercase symbols.
    pub fn depth_stream_url(&self) -> String {
        format!(
            "{}/ws/{}@depth@{}",
            self.ws_base,
            self.symbol.to_lowercase(),
            self.depth_speed
        )
    }

    /// Build the WebSocket stream URL for the Futures trade stream.
    ///
    /// Uses `btcusdt@trade` (not `@aggTrade` — Binance Futures uses
    /// `@trade` for individual trade events).
    pub fn trade_stream_url(&self) -> String {
        format!("{}/ws/{}@trade", self.ws_base, self.symbol.to_lowercase())
    }

    /// Build the REST depth endpoint URL.
    pub fn depth_rest_url(&self) -> String {
        format!(
            "{}/fapi/v1/depth?symbol={}&limit={}",
            self.rest_base, self.symbol, self.depth_limit
        )
    }

    /// Build the exchange info URL.
    pub fn exchange_info_url(&self) -> String {
        format!("{}/fapi/v1/exchangeInfo", self.rest_base)
    }

    /// Build the Phase 4 analytics configuration from the CLI arguments.
    pub fn analytics_config(&self) -> AnalyticsConfig {
        let default = AnalyticsConfig::btcusdt_default();
        AnalyticsConfig {
            analytics_version: self.analytics_version.clone(),
            tick_size_ticks: crate::orderbook::level::price_str_to_ticks(&self.tick_size)
                .unwrap_or(default.tick_size_ticks),
            large_trade_min_quantity_ticks: crate::analytics::config::btc_to_ticks(
                self.large_trade_btc,
            ),
            sweep_window_ms: self.sweep_window_ms,
            sweep_min_levels: self.sweep_min_levels,
            sweep_min_volume_ticks: crate::analytics::config::btc_to_ticks(
                self.sweep_min_volume_btc,
            ),
            absorption_window_ms: self.absorption_window_ms,
            absorption_min_volume_ticks: crate::analytics::config::btc_to_ticks(
                self.absorption_min_volume_btc,
            ),
            absorption_min_trades: self.absorption_min_trades,
            absorption_max_excursion_ticks: self.absorption_max_price_excursion_ticks,
            replenishment_window_ms: self.replenishment_window_ms,
            imbalance_depth: self.imbalance_depth_levels,
            snapshot_interval_ms: self.analytics_snapshot_interval_ms,
            aggregation_intervals_ms: default.aggregation_intervals_ms,
            heatmap_cell_ms: default.heatmap_cell_ms,
            retention_ms: self.analytics_retention_seconds * 1000,
            cluster_window_ms: default.cluster_window_ms,
            cluster_price_range_ticks: default.cluster_price_range_ticks,
            confidence_threshold: default.confidence_threshold,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            symbol: "BTCUSDT".to_string(),
            rest_base: "https://fapi.binance.com".to_string(),
            ws_base: "wss://fstream.binance.com".to_string(),
            depth_speed: "100ms".to_string(),
            depth_limit: 1000,
            reconnect_base_ms: 1000,
            reconnect_max_ms: 30_000,
            diagnostic_interval: 2,
            duration: 0,
            record: false,
            clickhouse_url: "http://localhost:8123".to_string(),
            clickhouse_database: "market_data".to_string(),
            clickhouse_user: String::new(),
            clickhouse_password: String::new(),
            batch_size: 1000,
            flush_interval_ms: 250,
            queue_capacity: 100_000,
            analytics: false,
            large_trade_btc: 5.0,
            sweep_window_ms: 100,
            sweep_min_levels: 3,
            sweep_min_volume_btc: 5.0,
            absorption_window_ms: 1000,
            absorption_min_volume_btc: 20.0,
            absorption_min_trades: 5,
            absorption_max_price_excursion_ticks: 3,
            replenishment_window_ms: 250,
            imbalance_depth_levels: 10,
            analytics_snapshot_interval_ms: 1000,
            analytics_retention_seconds: 900,
            tick_size: "0.10".to_string(),
            analytics_version: DEFAULT_ANALYTICS_VERSION.to_string(),
            command: None,
        }
    }
}

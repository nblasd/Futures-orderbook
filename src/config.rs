use clap::{Args, Parser, Subcommand};

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
            command: None,
        }
    }
}

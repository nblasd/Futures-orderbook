use clap::Parser;

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
        }
    }
}

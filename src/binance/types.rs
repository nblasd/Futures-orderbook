use serde::Deserialize;

/// REST response for GET /fapi/v1/depth
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DepthSnapshot {
    /// The last update ID in the snapshot.
    pub last_update_id: u64,
    /// Bid levels as [price_string, quantity_string].
    pub bids: Vec<(String, String)>,
    /// Ask levels as [price_string, quantity_string].
    pub asks: Vec<(String, String)>,
    /// Timestamp of the last update (from Binance Futures).
    #[serde(default)]
    pub t: Option<u64>,
    /// Event time (from Binance Futures).
    #[serde(default)]
    pub e: Option<String>,
    /// Last update time.
    #[serde(default)]
    pub last_update_time: Option<u64>,
}

/// WebSocket depth update event from Binance USDⓈ-M Futures.
///
/// Payload format from `btcusdt@depth@100ms`:
/// ```json
/// {
///   "e": "depthUpdate",
///   "E": 123456789,
///   "T": 123456788,
///   "s": "BTCUSDT",
///   "U": 150,
///   "u": 160,
///   "pu": 149,
///   "b": [["50000.10", "1.5"], ...],
///   "a": [["50000.20", "0.5"], ...]
/// }
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct DepthUpdate {
    /// Event type (e.g., "depthUpdate").
    #[serde(rename = "e")]
    pub event_type: String,

    /// Event time (millisecond timestamp).
    #[serde(rename = "E")]
    pub event_time: u64,

    /// Transaction time (millisecond timestamp).
    #[serde(rename = "T")]
    pub transaction_time: u64,

    /// Symbol (e.g., "BTCUSDT").
    #[serde(rename = "s")]
    pub symbol: String,

    /// First update ID in this event.
    #[serde(rename = "U")]
    pub first_update_id: u64,

    /// Final update ID in this event.
    #[serde(rename = "u")]
    pub final_update_id: u64,

    /// Final update ID of the previous stream event.
    /// This is the Futures-specific field that enables continuity validation.
    #[serde(rename = "pu")]
    pub previous_final_update_id: u64,

    /// Bid updates: [[price, quantity], ...]
    #[serde(rename = "b")]
    pub bids: Vec<(String, String)>,

    /// Ask updates: [[price, quantity], ...]
    #[serde(rename = "a")]
    pub asks: Vec<(String, String)>,
}

/// Exchange info symbol filter for price precision.
#[derive(Debug, Clone, Deserialize)]
pub struct ExchangeInfo {
    pub symbols: Vec<SymbolInfo>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SymbolInfo {
    pub symbol: String,
    pub filters: Vec<Filter>,
    #[serde(default)]
    pub base_asset_precision: Option<u32>,
    #[serde(default)]
    pub quote_asset_precision: Option<u32>,
    #[serde(default)]
    pub price_precision: Option<u32>,
    #[serde(default)]
    pub quantity_precision: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Filter {
    pub filter_type: String,
    #[serde(default)]
    pub min_price: Option<String>,
    #[serde(default)]
    pub max_price: Option<String>,
    #[serde(default)]
    pub tick_size: Option<String>,
    #[serde(default)]
    pub min_qty: Option<String>,
    #[serde(default)]
    pub max_qty: Option<String>,
    #[serde(default)]
    pub step_size: Option<String>,
}

impl SymbolInfo {
    /// Get the tick size for price precision.
    pub fn tick_size(&self) -> Option<&str> {
        self.filters
            .iter()
            .find(|f| f.filter_type == "PRICE_FILTER")
            .and_then(|f| f.tick_size.as_deref())
    }

    /// Get the step size for quantity precision.
    pub fn step_size(&self) -> Option<&str> {
        self.filters
            .iter()
            .find(|f| f.filter_type == "LOT_SIZE")
            .and_then(|f| f.step_size.as_deref())
    }
}

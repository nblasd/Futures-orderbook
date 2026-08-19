use serde::Deserialize;

/// Raw Binance USDⓈ-M Futures trade event from the `btcusdt@trade` stream.
///
/// Payload format (from live capture):
/// ```json
/// {
///   "e": "trade",
///   "E": 1787137583835,
///   "T": 1787137583835,
///   "s": "BTCUSDT",
///   "t": 7978350772,
///   "p": "64486.00",
///   "q": "0.002",
///   "X": "MARKET",
///   "m": false,
///   "st": 1
/// }
/// ```
///
/// Field semantics (from Binance Futures docs):
/// - `e`: Event type ("trade")
/// - `E`: Event time (milliseconds)
/// - `T`: Trade time (milliseconds)
/// - `s`: Symbol
/// - `t`: Trade ID
/// - `p`: Price (string)
/// - `q`: Quantity (string)
/// - `X`: Order type (e.g., "MARKET")
/// - `m`: Is buyer maker. `true` = buyer is the maker = aggressive SELL.
///   `false` = buyer is NOT the maker = aggressive BUY.
/// - `st`: Trade type
#[derive(Debug, Clone, Deserialize)]
pub struct FuturesTrade {
    /// Event type (e.g., "trade").
    #[serde(rename = "e")]
    pub event_type: String,

    /// Event time (millisecond timestamp).
    #[serde(rename = "E")]
    pub event_time: u64,

    /// Trade time (millisecond timestamp).
    #[serde(rename = "T")]
    pub trade_time: u64,

    /// Symbol (e.g., "BTCUSDT").
    #[serde(rename = "s")]
    pub symbol: String,

    /// Trade ID.
    #[serde(rename = "t")]
    pub trade_id: u64,

    /// Price (string to avoid floating-point issues).
    #[serde(rename = "p")]
    pub price: String,

    /// Quantity (string to avoid floating-point issues).
    #[serde(rename = "q")]
    pub quantity: String,

    /// Order type (e.g., "MARKET").
    #[serde(rename = "X")]
    pub order_type: String,

    /// Is buyer maker.
    ///
    /// This is the critical field for aggressor-side classification:
    /// - `true`  → buyer is the maker → seller is the aggressor → **aggressive SELL**
    /// - `false` → buyer is NOT the maker → buyer is the aggressor → **aggressive BUY**
    #[serde(rename = "m")]
    pub is_buyer_maker: bool,

    /// Trade type.
    #[serde(rename = "st", default)]
    pub trade_type: u32,
}

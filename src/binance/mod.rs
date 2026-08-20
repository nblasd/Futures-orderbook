pub mod rest;
pub mod trade_types;
pub mod types;
pub mod websocket;
pub mod ws_trades;

pub use rest::RestClient;
pub use trade_types::FuturesTrade;

pub use websocket::{WebSocketClient, WsMessage};
pub use ws_trades::{TradeWebSocketClient, TradeWsMessage};

/// Current wall-clock time in nanoseconds since the Unix epoch.
pub fn now_unix_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

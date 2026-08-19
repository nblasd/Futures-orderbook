use thiserror::Error;

/// Errors specific to the order-book engine.
#[derive(Debug, Error)]
pub enum OrderBookError {
    #[error("sequence gap: expected update_id {expected}, got {got}")]
    SequenceGap { expected: u64, got: u64 },

    #[error("pu continuity failure: expected pu={expected}, got pu={got}")]
    PuContinuityFailure { expected: u64, got: u64 },

    #[error("order book is not synchronized (state: {0:?})")]
    NotSynchronized(crate::orderbook::synchronizer::SyncState),

    #[error("invalid price level: {0}")]
    InvalidPriceLevel(String),

    #[error("snapshot already applied")]
    SnapshotAlreadyApplied,

    #[error("snapshot required before depth updates")]
    SnapshotRequired,
}

/// Errors related to Binance API connectivity.
#[derive(Debug, Error)]
pub enum BinanceError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("WebSocket error: {0}")]
    WebSocket(String),

    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("invalid response: {0}")]
    InvalidResponse(String),

    #[error("connection closed")]
    ConnectionClosed,

    #[error("unexpected message format")]
    UnexpectedMessageFormat,

    #[error("symbol not found: {0}")]
    SymbolNotFound(String),
}

pub type Result<T> = std::result::Result<T, anyhow::Error>;

//! Derived analytical events produced by the Phase 4 analytics engine.
//!
//! These are deliberately separate from the raw [`MarketEvent`] types. Raw
//! events describe *what happened on the wire*; analytics events describe
//! *what the analytics engine concluded* from them.

use serde::Serialize;

/// The kind of a derived analytics event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum AnalyticsEventKind {
    /// An aggressive trade contributed to flow (delta/CVD).
    TradeDelta,
    /// A trade at/above the configured large-trade threshold.
    LargeTrade,
    /// Displayed liquidity appeared at a previously-empty price level.
    LiquidityAdded,
    /// Displayed liquidity vanished (level removed).
    LiquidityRemoved,
    /// Displayed liquidity increased at an existing level.
    LiquidityIncreased,
    /// Displayed liquidity decreased at an existing level.
    LiquidityDecreased,
    /// A decrease followed by an increase at the same level within the
    /// configured window. Candidate flag only — does NOT prove an iceberg.
    LiquidityReplenishment,
    /// Aggressive trades consumed liquidity across several price levels.
    SweepCandidate,
    /// Heavy aggressive flow absorbed by opposing liquidity without price
    /// displacement. Candidate classification — not proof of hidden liquidity.
    AbsorptionCandidate,
    /// A completed trade cluster.
    Cluster,
    /// A data-quality anomaly was observed (e.g. crossed book).
    BookAnomaly,
}

impl AnalyticsEventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            AnalyticsEventKind::TradeDelta => "trade_delta",
            AnalyticsEventKind::LargeTrade => "large_trade",
            AnalyticsEventKind::LiquidityAdded => "liquidity_added",
            AnalyticsEventKind::LiquidityRemoved => "liquidity_removed",
            AnalyticsEventKind::LiquidityIncreased => "liquidity_increased",
            AnalyticsEventKind::LiquidityDecreased => "liquidity_decreased",
            AnalyticsEventKind::LiquidityReplenishment => "liquidity_replenishment",
            AnalyticsEventKind::SweepCandidate => "sweep_candidate",
            AnalyticsEventKind::AbsorptionCandidate => "absorption_candidate",
            AnalyticsEventKind::Cluster => "cluster",
            AnalyticsEventKind::BookAnomaly => "book_anomaly",
        }
    }
}

impl std::fmt::Display for AnalyticsEventKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A derived analytics event.
#[derive(Debug, Clone, Serialize)]
pub struct AnalyticsEvent {
    /// Symbol (BTCUSDT).
    pub symbol: String,
    /// Exchange event timestamp (ms).
    pub ts_ms: u64,
    /// Event kind.
    pub kind: AnalyticsEventKind,
    /// Side discriminator: "BID"/"ASK" for liquidity, "BUY"/"SELL" for flow.
    pub side: Option<String>,
    /// Price in integer ticks (when applicable).
    pub price: Option<u64>,
    /// Quantity in integer ticks (when applicable).
    pub quantity: u64,
    /// Free-form evidence (deterministic, JSON-serializable).
    pub detail: serde_json::Value,
}

impl AnalyticsEvent {
    pub fn new(kind: AnalyticsEventKind, ts_ms: u64, symbol: &str) -> Self {
        Self {
            symbol: symbol.to_string(),
            ts_ms,
            kind,
            side: None,
            price: None,
            quantity: 0,
            detail: serde_json::Value::Null,
        }
    }

    pub fn with_side(mut self, side: impl Into<String>) -> Self {
        self.side = Some(side.into());
        self
    }

    pub fn with_price(mut self, price: u64) -> Self {
        self.price = Some(price);
        self
    }

    pub fn with_quantity(mut self, quantity: u64) -> Self {
        self.quantity = quantity;
        self
    }

    pub fn with_detail(mut self, detail: serde_json::Value) -> Self {
        self.detail = detail;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kind_strings() {
        assert_eq!(
            AnalyticsEventKind::SweepCandidate.as_str(),
            "sweep_candidate"
        );
        assert_eq!(
            AnalyticsEventKind::AbsorptionCandidate.as_str(),
            "absorption_candidate"
        );
        assert_eq!(
            AnalyticsEventKind::LiquidityReplenishment.as_str(),
            "liquidity_replenishment"
        );
    }
}

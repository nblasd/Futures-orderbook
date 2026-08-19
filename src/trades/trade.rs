/// Aggressor-side classification for executed trades.
///
/// Derived from Binance's `m` (is buyer maker) field:
///
/// | Binance `m` | Buyer is maker? | Aggressor | AggressorSide |
/// |-------------|-----------------|-----------|---------------|
/// | `true`      | Yes             | Seller    | Sell          |
/// | `false`     | No              | Buyer     | Buy           |
///
/// The aggressor is the party that crossed the spread — i.e., the one who
/// submitted a marketable order that consumed resting liquidity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggressorSide {
    /// Aggressive buyer consumed resting asks.
    Buy,
    /// Aggressive seller consumed resting bids.
    Sell,
}

impl AggressorSide {
    /// Classify aggressor from Binance's `is_buyer_maker` flag.
    ///
    /// - `is_buyer_maker = true`  → buyer is the maker → seller aggressed → `Sell`
    /// - `is_buyer_maker = false` → buyer is NOT the maker → buyer aggressed → `Buy`
    pub fn from_buyer_maker(is_buyer_maker: bool) -> Self {
        if is_buyer_maker {
            AggressorSide::Sell
        } else {
            AggressorSide::Buy
        }
    }

    /// Display label for diagnostics.
    pub fn label(self) -> &'static str {
        match self {
            AggressorSide::Buy => "BUY",
            AggressorSide::Sell => "SELL",
        }
    }
}

impl std::fmt::Display for AggressorSide {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.label())
    }
}

/// A normalized trade event, transport-independent.
///
/// This is the internal representation that all future analytics (CVD,
/// delta, absorption, etc.) will consume.
#[derive(Debug, Clone)]
pub struct TradeEvent {
    /// Symbol (e.g., "BTCUSDT").
    pub symbol: String,
    /// Aggregate/trade ID from the exchange.
    pub trade_id: u64,
    /// Price in integer ticks (same representation as order-book price).
    pub price_ticks: u64,
    /// Quantity in integer ticks (same representation as order-book quantity).
    pub quantity_ticks: u64,
    /// Event time from the exchange (milliseconds).
    pub event_time: u64,
    /// Trade time from the exchange (milliseconds).
    pub trade_time: u64,
    /// Local monotonic timestamp when this message was received (nanoseconds).
    pub local_receive_time_ns: u128,
    /// Aggressor side.
    pub aggressor: AggressorSide,
    /// Order type (e.g., "MARKET").
    pub order_type: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buyer_maker_true_is_sell() {
        assert_eq!(AggressorSide::from_buyer_maker(true), AggressorSide::Sell);
    }

    #[test]
    fn test_buyer_maker_false_is_buy() {
        assert_eq!(AggressorSide::from_buyer_maker(false), AggressorSide::Buy);
    }

    #[test]
    fn test_aggressor_labels() {
        assert_eq!(AggressorSide::Buy.label(), "BUY");
        assert_eq!(AggressorSide::Sell.label(), "SELL");
    }

    #[test]
    fn test_aggressor_display() {
        assert_eq!(format!("{}", AggressorSide::Buy), "BUY");
        assert_eq!(format!("{}", AggressorSide::Sell), "SELL");
    }
}

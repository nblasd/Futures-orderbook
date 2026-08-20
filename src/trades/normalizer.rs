use crate::binance::trade_types::FuturesTrade;
use crate::orderbook::level::{price_str_to_ticks, quantity_str_to_ticks};

use super::trade::{AggressorSide, TradeEvent};

/// Result of attempting to normalize a raw trade.
#[derive(Debug, Clone)]
pub enum NormalizeResult {
    /// Successfully normalized into a TradeEvent.
    Ok(TradeEvent),
    /// Not a real trade — a synthetic marker event (e.g., funding, position mark).
    /// Identified by zero price and/or zero quantity with order_type "NA".
    MarkerEvent(FuturesTrade),
    /// Failed to parse — malformed price, quantity, or missing fields.
    ParseError(String),
}

/// Check if a raw FuturesTrade is a synthetic marker event rather than
/// an actual executable trade.
///
/// On Binance Futures, the `btcusdt@trade` stream occasionally delivers
/// non-trade marker events with `p: "0"`, `q: "0"`, `X: "NA"`. These
/// represent funding settlements, position marks, or similar bookkeeping
/// events — not consumable trades.
pub fn is_marker_event(trade: &FuturesTrade) -> bool {
    // A real BTCUSDT trade must have a nonzero price and quantity.
    // Synthetic markers have "0" for both, and "NA" for order type.
    trade.price == "0"
        || trade.price == "0.0"
        || trade.price == "0.00"
        || trade.quantity == "0"
        || trade.quantity == "0.0"
        || trade.quantity == "0.00"
        || trade.order_type == "NA"
}

/// Normalize a raw Binance Futures trade into our internal TradeEvent.
///
/// Returns `NormalizeResult::MarkerEvent` for synthetic non-trade events
/// (zero price/quantity with order_type "NA").
///
/// This is a pure function with no side effects — testable without network.
pub fn normalize_trade(trade: &FuturesTrade) -> NormalizeResult {
    // Reject synthetic marker events first
    if is_marker_event(trade) {
        return NormalizeResult::MarkerEvent(trade.clone());
    }

    let price_ticks = match price_str_to_ticks(&trade.price) {
        Ok(t) => t,
        Err(e) => {
            return NormalizeResult::ParseError(format!("invalid price '{}': {}", trade.price, e))
        }
    };
    let quantity_ticks = match quantity_str_to_ticks(&trade.quantity) {
        Ok(t) => t,
        Err(e) => {
            return NormalizeResult::ParseError(format!(
                "invalid quantity '{}': {}",
                trade.quantity, e
            ))
        }
    };
    let aggressor = AggressorSide::from_buyer_maker(trade.is_buyer_maker);

    NormalizeResult::Ok(TradeEvent {
        symbol: trade.symbol.clone(),
        trade_id: trade.trade_id,
        price_ticks,
        quantity_ticks,
        event_time: trade.event_time,
        trade_time: trade.trade_time,
        local_receive_time_ns: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
        aggressor,
        order_type: trade.order_type.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binance::trade_types::FuturesTrade;

    fn make_trade(trade_id: u64, price: &str, qty: &str, is_buyer_maker: bool) -> FuturesTrade {
        FuturesTrade {
            event_type: "trade".to_string(),
            event_time: 1787137583835,
            trade_time: 1787137583835,
            symbol: "BTCUSDT".to_string(),
            trade_id,
            price: price.to_string(),
            quantity: qty.to_string(),
            order_type: "MARKET".to_string(),
            is_buyer_maker,
            trade_type: 1,
        }
    }

    fn unwrap_trade(result: NormalizeResult) -> TradeEvent {
        match result {
            NormalizeResult::Ok(t) => t,
            other => panic!("Expected Ok, got {:?}", other),
        }
    }

    #[test]
    fn test_normalize_buy_aggressor() {
        let raw = make_trade(12345, "64486.00", "0.002", false);
        let event = unwrap_trade(normalize_trade(&raw));
        assert_eq!(event.trade_id, 12345);
        assert_eq!(event.price_ticks, price_str_to_ticks("64486.00").unwrap());
        assert_eq!(
            event.quantity_ticks,
            quantity_str_to_ticks("0.002").unwrap()
        );
        assert_eq!(event.aggressor, AggressorSide::Buy);
        assert_eq!(event.symbol, "BTCUSDT");
    }

    #[test]
    fn test_normalize_sell_aggressor() {
        let raw = make_trade(12346, "64485.50", "0.100", true);
        let event = unwrap_trade(normalize_trade(&raw));
        assert_eq!(event.aggressor, AggressorSide::Sell);
    }

    #[test]
    fn test_price_matches_orderbook_representation() {
        let raw = make_trade(1, "50000.10", "1.0", false);
        let event = unwrap_trade(normalize_trade(&raw));
        assert_eq!(event.price_ticks, price_str_to_ticks("50000.10").unwrap());
        assert_eq!(event.quantity_ticks, quantity_str_to_ticks("1.0").unwrap());
    }

    #[test]
    fn test_quantity_is_exact() {
        let raw = make_trade(1, "100.00", "0.001", false);
        let event = unwrap_trade(normalize_trade(&raw));
        assert_eq!(event.quantity_ticks, 100_000);
    }

    #[test]
    fn test_timestamps_preserved() {
        let mut raw = make_trade(1, "100.00", "0.001", false);
        raw.event_time = 999;
        raw.trade_time = 888;
        let event = unwrap_trade(normalize_trade(&raw));
        assert_eq!(event.event_time, 999);
        assert_eq!(event.trade_time, 888);
    }

    #[test]
    fn test_local_receive_time_set() {
        let raw = make_trade(1, "100.00", "0.001", false);
        let event = unwrap_trade(normalize_trade(&raw));
        assert!(event.local_receive_time_ns > 0);
    }

    #[test]
    fn test_trade_id_preserved() {
        let raw = make_trade(999999, "100.00", "0.001", false);
        let event = unwrap_trade(normalize_trade(&raw));
        assert_eq!(event.trade_id, 999999);
    }

    #[test]
    fn test_order_type_preserved() {
        let mut raw = make_trade(1, "100.00", "0.001", false);
        raw.order_type = "LIMIT".to_string();
        let event = unwrap_trade(normalize_trade(&raw));
        assert_eq!(event.order_type, "LIMIT");
    }

    #[test]
    fn test_invalid_price_returns_parse_error() {
        let raw = make_trade(1, "not_a_number", "0.001", false);
        assert!(matches!(
            normalize_trade(&raw),
            NormalizeResult::ParseError(_)
        ));
    }

    #[test]
    fn test_invalid_quantity_returns_parse_error() {
        let raw = make_trade(1, "100.00", "abc", false);
        assert!(matches!(
            normalize_trade(&raw),
            NormalizeResult::ParseError(_)
        ));
    }

    // --- Marker event rejection tests ---

    #[test]
    fn test_zero_price_zero_quantity_is_marker() {
        let raw = make_trade(7979198979, "0", "0", true);
        assert!(is_marker_event(&raw));
        assert!(matches!(
            normalize_trade(&raw),
            NormalizeResult::MarkerEvent(_)
        ));
    }

    #[test]
    fn test_na_order_type_is_marker() {
        let mut raw = make_trade(1, "65000.00", "0.001", false);
        raw.order_type = "NA".to_string();
        assert!(is_marker_event(&raw));
    }

    #[test]
    fn test_normal_trade_is_not_marker() {
        let raw = make_trade(1, "65000.00", "0.001", false);
        assert!(!is_marker_event(&raw));
    }

    #[test]
    fn test_zero_price_only_is_marker() {
        let raw = make_trade(1, "0", "0.001", false);
        assert!(is_marker_event(&raw));
    }

    #[test]
    fn test_zero_quantity_only_is_marker() {
        let raw = make_trade(1, "65000.00", "0", false);
        assert!(is_marker_event(&raw));
    }
}

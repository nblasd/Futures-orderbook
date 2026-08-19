use crate::binance::trade_types::FuturesTrade;
use crate::orderbook::level::{price_str_to_ticks, quantity_str_to_ticks};

use super::trade::{AggressorSide, TradeEvent};

/// Normalize a raw Binance Futures trade into our internal TradeEvent.
///
/// This is a pure function with no side effects — testable without network.
pub fn normalize_trade(trade: &FuturesTrade) -> anyhow::Result<TradeEvent> {
    let price_ticks = price_str_to_ticks(&trade.price)?;
    let quantity_ticks = quantity_str_to_ticks(&trade.quantity)?;
    let aggressor = AggressorSide::from_buyer_maker(trade.is_buyer_maker);

    Ok(TradeEvent {
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
    use crate::orderbook::level::TICK_SCALE;

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

    #[test]
    fn test_normalize_buy_aggressor() {
        let raw = make_trade(12345, "64486.00", "0.002", false);
        let event = normalize_trade(&raw).unwrap();
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
        let event = normalize_trade(&raw).unwrap();
        assert_eq!(event.aggressor, AggressorSide::Sell);
    }

    #[test]
    fn test_price_matches_orderbook_representation() {
        let raw = make_trade(1, "50000.10", "1.0", false);
        let event = normalize_trade(&raw).unwrap();
        // Must be identical to what orderbook::level would produce
        assert_eq!(event.price_ticks, price_str_to_ticks("50000.10").unwrap());
        assert_eq!(event.quantity_ticks, quantity_str_to_ticks("1.0").unwrap());
    }

    #[test]
    fn test_quantity_is_exact() {
        let raw = make_trade(1, "100.00", "0.001", false);
        let event = normalize_trade(&raw).unwrap();
        // 0.001 * 1e8 = 100000
        assert_eq!(event.quantity_ticks, 100_000);
    }

    #[test]
    fn test_timestamps_preserved() {
        let mut raw = make_trade(1, "100.00", "0.001", false);
        raw.event_time = 999;
        raw.trade_time = 888;
        let event = normalize_trade(&raw).unwrap();
        assert_eq!(event.event_time, 999);
        assert_eq!(event.trade_time, 888);
    }

    #[test]
    fn test_local_receive_time_set() {
        let raw = make_trade(1, "100.00", "0.001", false);
        let event = normalize_trade(&raw).unwrap();
        assert!(event.local_receive_time_ns > 0);
    }

    #[test]
    fn test_trade_id_preserved() {
        let raw = make_trade(999999, "100.00", "0.001", false);
        let event = normalize_trade(&raw).unwrap();
        assert_eq!(event.trade_id, 999999);
    }

    #[test]
    fn test_order_type_preserved() {
        let mut raw = make_trade(1, "100.00", "0.001", false);
        raw.order_type = "LIMIT".to_string();
        let event = normalize_trade(&raw).unwrap();
        assert_eq!(event.order_type, "LIMIT");
    }

    #[test]
    fn test_invalid_price_returns_error() {
        let raw = make_trade(1, "not_a_number", "0.001", false);
        assert!(normalize_trade(&raw).is_err());
    }

    #[test]
    fn test_invalid_quantity_returns_error() {
        let raw = make_trade(1, "100.00", "abc", false);
        assert!(normalize_trade(&raw).is_err());
    }
}

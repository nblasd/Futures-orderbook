//! Large-trade detection.
//!
//! A trade is "large" when its quantity is at/above a configurable absolute
//! threshold (e.g. `--large-trade-btc 5.0`). The threshold is not hardcoded
//! because the market regime changes.

use std::collections::VecDeque;

use crate::analytics::config::AnalyticsConfig;
use crate::analytics::events::{AnalyticsEvent, AnalyticsEventKind};
use crate::trades::trade::{AggressorSide, TradeEvent};

/// A detected large trade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LargeTrade {
    pub ts_ms: u64,
    pub price: u64,
    pub quantity: u64,
    pub side: AggressorSide,
    pub trade_id: u64,
    /// Notional as an exact product `price_ticks * quantity_ticks`.
    pub notional_ticks: u128,
    pub threshold_ticks: u64,
}

impl LargeTrade {
    /// Notional in USDT (display only; the exact product is `notional_ticks`).
    pub fn notional_usdt_f64(&self) -> f64 {
        self.notional_ticks as f64 / (crate::orderbook::level::TICK_SCALE as f64).powi(2)
    }
}

/// Large-trade detector.
pub struct LargeTradeDetector {
    threshold_ticks: u64,
    pub count: u64,
    /// Bounded recent large-trade list for diagnostics.
    recent: VecDeque<LargeTrade>,
}

impl LargeTradeDetector {
    pub fn new(cfg: &AnalyticsConfig) -> Self {
        Self {
            threshold_ticks: cfg.large_trade_min_quantity_ticks,
            count: 0,
            recent: VecDeque::new(),
        }
    }

    /// Check a trade. Returns a `LargeTrade` analytics event when it crosses
    /// the threshold.
    pub fn check(&mut self, trade: &TradeEvent) -> Option<AnalyticsEvent> {
        if trade.quantity_ticks < self.threshold_ticks {
            return None;
        }
        self.count += 1;
        let lt = LargeTrade {
            ts_ms: trade.trade_time,
            price: trade.price_ticks,
            quantity: trade.quantity_ticks,
            side: trade.aggressor,
            trade_id: trade.trade_id,
            notional_ticks: trade.price_ticks as u128 * trade.quantity_ticks as u128,
            threshold_ticks: self.threshold_ticks,
        };
        self.recent.push_back(lt.clone());
        if self.recent.len() > 512 {
            self.recent.pop_front();
        }
        Some(
            AnalyticsEvent::new(AnalyticsEventKind::LargeTrade, lt.ts_ms, &trade.symbol)
                .with_side(lt.side.label())
                .with_price(lt.price)
                .with_quantity(lt.quantity)
                .with_detail(serde_json::json!({
                    "trade_id": lt.trade_id,
                    "notional_ticks": lt.notional_ticks.to_string(),
                    "threshold_ticks": lt.threshold_ticks,
                })),
        )
    }

    pub fn recent_large_trades(&self) -> impl Iterator<Item = &LargeTrade> {
        self.recent.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trades::trade::TradeEvent;

    fn trade(id: u64, price: &str, qty: &str, side: AggressorSide, ts: u64) -> TradeEvent {
        TradeEvent {
            symbol: "BTCUSDT".to_string(),
            trade_id: id,
            price_ticks: crate::orderbook::level::price_str_to_ticks(price).unwrap(),
            quantity_ticks: crate::orderbook::level::quantity_str_to_ticks(qty).unwrap(),
            event_time: ts,
            trade_time: ts,
            local_receive_time_ns: 0,
            aggressor: side,
            order_type: "MARKET".to_string(),
        }
    }

    #[test]
    fn test_large_trade_detection() {
        let mut det = LargeTradeDetector::new(&AnalyticsConfig::btcusdt_default());
        // 5 BTC threshold: 4.9 not large, 5.0 large.
        assert!(det
            .check(&trade(1, "68000.00", "4.9", AggressorSide::Buy, 1))
            .is_none());
        let ev = det
            .check(&trade(2, "68000.00", "5.0", AggressorSide::Sell, 2))
            .unwrap();
        assert_eq!(ev.kind, AnalyticsEventKind::LargeTrade);
        assert_eq!(det.count, 1);
        let lt = det.recent_large_trades().next().unwrap();
        assert_eq!(lt.side, AggressorSide::Sell);
        assert_eq!(lt.quantity, 500_000_000);
        assert_eq!(lt.threshold_ticks, 500_000_000);
    }
}

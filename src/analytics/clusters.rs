//! Trade clustering.
//!
//! A cluster groups nearby aggressive trades occurring within a configurable
//! time window and price range. Clusters are the raw material for sweep and
//! absorption classification.

use std::collections::VecDeque;

use crate::analytics::config::AnalyticsConfig;
use crate::trades::trade::{AggressorSide, TradeEvent};

/// A closed trade cluster.
#[derive(Debug, Clone, PartialEq)]
pub struct TradeCluster {
    pub start_ms: u64,
    pub end_ms: u64,
    pub high: u64,
    pub low: u64,
    pub total_volume: u64,
    pub buy_volume: u64,
    pub sell_volume: u64,
    pub delta: i128,
    pub trade_count: u64,
    pub largest_trade_quantity: u64,
    pub dominant_side: AggressorSide,
    /// Fraction of volume on the dominant side (0..1).
    pub aggressor_dominance: f64,
    /// Distinct price levels touched.
    pub levels_crossed: u32,
    /// Monotonic directional progression flag.
    pub monotonic: bool,
    /// First price in the cluster.
    pub first_price: u64,
    /// Last price in the cluster.
    pub last_price: u64,
}

impl TradeCluster {
    pub fn duration_ms(&self) -> u64 {
        self.end_ms.saturating_sub(self.start_ms)
    }
}

/// Trade-cluster tracker.
pub struct ClusterTracker {
    window_ms: u64,
    price_range_ticks: u64,
    open: Option<TradeCluster>,
    /// Closed clusters (bounded, retained within the retention window).
    closed: VecDeque<TradeCluster>,
    retention_ms: u64,
}

impl ClusterTracker {
    pub fn new(cfg: &AnalyticsConfig) -> Self {
        Self {
            window_ms: cfg.cluster_window_ms,
            price_range_ticks: cfg.cluster_price_range_raw(),
            open: None,
            closed: VecDeque::new(),
            retention_ms: cfg.retention_ms,
        }
    }

    /// Feed a trade. Returns the cluster that was closed (if this trade ended
    /// the previous cluster).
    pub fn on_trade(&mut self, trade: &TradeEvent) -> Option<TradeCluster> {
        let ts = trade.trade_time;
        let price = trade.price_ticks;

        let fits = match self.open.as_ref() {
            Some(c) => {
                let within_time = ts.saturating_sub(c.end_ms) <= self.window_ms;
                let within_price = price.abs_diff(c.last_price) <= self.price_range_ticks;
                within_time && within_price
            }
            None => false,
        };

        let mut closed = None;
        if !fits {
            closed = self.open.take();
            if let Some(c) = &closed {
                self.closed.push_back(c.clone());
            }
            self.open = Some(TradeCluster {
                start_ms: ts,
                end_ms: ts,
                high: price,
                low: price,
                total_volume: 0,
                buy_volume: 0,
                sell_volume: 0,
                delta: 0,
                trade_count: 0,
                largest_trade_quantity: 0,
                dominant_side: trade.aggressor,
                aggressor_dominance: 0.0,
                levels_crossed: 1,
                monotonic: true,
                first_price: price,
                last_price: price,
            });
        }

        let c = self.open.as_mut().unwrap();
        c.end_ms = ts;
        c.high = c.high.max(price);
        c.low = c.low.min(price);
        c.total_volume += trade.quantity_ticks;
        c.trade_count += 1;
        match trade.aggressor {
            AggressorSide::Buy => {
                c.buy_volume += trade.quantity_ticks;
                c.delta += trade.quantity_ticks as i128;
            }
            AggressorSide::Sell => {
                c.sell_volume += trade.quantity_ticks;
                c.delta -= trade.quantity_ticks as i128;
            }
        }
        c.largest_trade_quantity = c.largest_trade_quantity.max(trade.quantity_ticks);
        if price != c.last_price {
            c.levels_crossed += 1;
            // Monotonicity: BUY requires non-decreasing, SELL non-increasing.
            let dir_ok = match c.dominant_side {
                AggressorSide::Buy => price >= c.last_price,
                AggressorSide::Sell => price <= c.last_price,
            };
            c.monotonic = c.monotonic && dir_ok;
        }
        c.last_price = price;

        // Dominance & side by volume.
        let (dom_vol, side) = if c.buy_volume >= c.sell_volume {
            (c.buy_volume, AggressorSide::Buy)
        } else {
            (c.sell_volume, AggressorSide::Sell)
        };
        c.dominant_side = side;
        c.aggressor_dominance = if c.total_volume > 0 {
            dom_vol as f64 / c.total_volume as f64
        } else {
            0.0
        };

        closed
    }

    /// Close the current open cluster (e.g. at flush time).
    pub fn close_open(&mut self) -> Option<TradeCluster> {
        let c = self.open.take();
        if let Some(c) = &c {
            self.closed.push_back(c.clone());
        }
        c
    }

    pub fn closed_clusters(&self) -> impl Iterator<Item = &TradeCluster> {
        self.closed.iter()
    }

    /// Drop closed clusters older than the retention window.
    pub fn prune(&mut self, now_ms: u64) {
        let cutoff = now_ms.saturating_sub(self.retention_ms);
        while let Some(front) = self.closed.front() {
            if front.end_ms < cutoff {
                self.closed.pop_front();
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trades::trade::TradeEvent;

    fn trade(id: u64, price_ticks: u64, qty: u64, side: AggressorSide, ts: u64) -> TradeEvent {
        TradeEvent {
            symbol: "BTCUSDT".to_string(),
            trade_id: id,
            price_ticks,
            quantity_ticks: qty,
            event_time: ts,
            trade_time: ts,
            local_receive_time_ns: 0,
            aggressor: side,
            order_type: "MARKET".to_string(),
        }
    }

    #[test]
    fn test_cluster_groups_nearby_trades() {
        let mut ct = ClusterTracker::new(&AnalyticsConfig::btcusdt_default());
        // 68000.00 tick
        let p = 6_800_000_000_000u64;
        let t1 = trade(1, p, 100, AggressorSide::Buy, 1000);
        let t2 = trade(2, p, 200, AggressorSide::Buy, 1050);
        let t3 = trade(3, p, 300, AggressorSide::Sell, 5000); // far in time
        assert!(ct.on_trade(&t1).is_none());
        assert!(ct.on_trade(&t2).is_none());
        let closed = ct.on_trade(&t3).unwrap();
        assert_eq!(closed.trade_count, 2);
        assert_eq!(closed.total_volume, 300);
        assert_eq!(closed.delta, 300); // buy 100+200
        assert_eq!(closed.levels_crossed, 1);
    }
}

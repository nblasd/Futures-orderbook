//! Volume/flow heatmap data model.
//!
//! The heatmap tracks, per price level and time bucket, the trade flow
//! (volume, buy/sell split) plus a simple liquidity footprint. It is a
//! bounded in-memory model with a configurable retention window; the
//! per-cell aggregation is emitted as part of the analytics snapshot.
//!
//! No drawing or charting is produced here — this module owns the **data
//! model** only.

use std::collections::BTreeMap;

use crate::analytics::config::AnalyticsConfig;
use crate::trades::trade::{AggressorSide, TradeEvent};

/// One heatmap cell: flow at a price within a time bucket.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HeatmapCell {
    pub price: u64,
    pub buy_volume: u64,
    pub sell_volume: u64,
    pub total_volume: u64,
    pub delta: i128,
    pub trade_count: u64,
}

impl HeatmapCell {
    pub fn delta_f64(&self) -> f64 {
        self.delta as f64
    }
}

/// The heatmap: time bucket → (price → cell).
pub struct Heatmap {
    /// Bucket start → price → cell. Bounded by retention.
    buckets: BTreeMap<u64, BTreeMap<u64, HeatmapCell>>,
    cell_ms: u64,
    retention_ms: u64,
}

impl Heatmap {
    pub fn new(cfg: &AnalyticsConfig) -> Self {
        Self {
            buckets: BTreeMap::new(),
            cell_ms: cfg.heatmap_cell_ms,
            retention_ms: cfg.retention_ms,
        }
    }

    /// Feed a trade into the heatmap.
    pub fn on_trade(&mut self, trade: &TradeEvent) {
        let ts = trade.trade_time;
        let bucket_start = ts - (ts % self.cell_ms);
        let price = trade.price_ticks;
        let cell = self
            .buckets
            .entry(bucket_start)
            .or_default()
            .entry(price)
            .or_insert_with(|| HeatmapCell {
                price,
                ..Default::default()
            });
        cell.total_volume += trade.quantity_ticks;
        cell.trade_count += 1;
        match trade.aggressor {
            AggressorSide::Buy => {
                cell.buy_volume += trade.quantity_ticks;
                cell.delta += trade.quantity_ticks as i128;
            }
            AggressorSide::Sell => {
                cell.sell_volume += trade.quantity_ticks;
                cell.delta -= trade.quantity_ticks as i128;
            }
        }
    }

    /// Iterate over buckets (oldest first) with their price→cell maps.
    pub fn buckets(&self) -> impl Iterator<Item = (u64, &BTreeMap<u64, HeatmapCell>)> {
        self.buckets.iter().map(|(ts, cells)| (*ts, cells))
    }

    /// Total flow in a price range within the last `window_ms`.
    pub fn range_volume(
        &self,
        lo: u64,
        hi: u64,
        now_ms: u64,
        window_ms: u64,
    ) -> (u64, u64, u64, i128) {
        let cutoff = now_ms.saturating_sub(window_ms);
        let mut buy = 0u64;
        let mut sell = 0u64;
        let mut total = 0u64;
        let mut delta = 0i128;
        for (bucket_start, cells) in self.buckets.iter() {
            if *bucket_start < cutoff {
                continue;
            }
            for (price, cell) in cells.range(lo..=hi) {
                let _ = price;
                buy += cell.buy_volume;
                sell += cell.sell_volume;
                total += cell.total_volume;
                delta += cell.delta;
            }
        }
        (buy, sell, total, delta)
    }

    /// Drop buckets older than the retention window.
    pub fn prune(&mut self, now_ms: u64) {
        let cutoff = now_ms.saturating_sub(self.retention_ms);
        let stale: Vec<u64> = self
            .buckets
            .iter()
            .filter(|(ts, _)| **ts < cutoff)
            .map(|(ts, _)| *ts)
            .collect();
        for ts in stale {
            self.buckets.remove(&ts);
        }
    }

    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    pub fn cell_count(&self) -> usize {
        self.buckets.values().map(|c| c.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trades::trade::TradeEvent;

    fn trade(id: u64, price: u64, qty: u64, side: AggressorSide, ts: u64) -> TradeEvent {
        TradeEvent {
            symbol: "BTCUSDT".to_string(),
            trade_id: id,
            price_ticks: price,
            quantity_ticks: qty,
            event_time: ts,
            trade_time: ts,
            local_receive_time_ns: 0,
            aggressor: side,
            order_type: "MARKET".to_string(),
        }
    }

    #[test]
    fn test_heatmap_aggregates_by_bucket() {
        let cfg = AnalyticsConfig::btcusdt_default();
        let mut hm = Heatmap::new(&cfg);
        let p = 6_800_000_000_000u64;
        hm.on_trade(&trade(1, p, 100, AggressorSide::Buy, 0));
        hm.on_trade(&trade(2, p, 200, AggressorSide::Sell, 500)); // same 1s bucket
        hm.on_trade(&trade(3, p, 300, AggressorSide::Buy, 2500)); // next bucket
        assert_eq!(hm.bucket_count(), 2);
        let (buy, sell, total, delta) = hm.range_volume(p, p, 4000, 5000);
        assert_eq!(buy, 400);
        assert_eq!(sell, 200);
        assert_eq!(total, 600);
        assert_eq!(delta, 200);
    }

    #[test]
    fn test_prune_bounded() {
        let cfg = AnalyticsConfig::btcusdt_default();
        let mut hm = Heatmap::new(&cfg);
        let p = 6_800_000_000_000u64;
        hm.on_trade(&trade(1, p, 100, AggressorSide::Buy, 0));
        hm.on_trade(&trade(2, p, 100, AggressorSide::Buy, 30_000_000)); // 8.3h later
        hm.prune(30_001_000);
        assert_eq!(hm.bucket_count(), 1);
    }
}

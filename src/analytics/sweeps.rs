//! Sweep candidate detection.
//!
//! A sweep candidate occurs when aggressive trades consume liquidity across
//! multiple consecutive price levels in a short time interval. Detection is
//! **conservative**: not every sequence of trades is a sweep. A cluster of
//! same-side aggressive trades that:
//!
//! 1. touches at least `sweep_min_levels` distinct price levels,
//! 2. progresses directionally (BUY non-decreasing / SELL non-increasing),
//! 3. has meaningful aggregate volume,
//! 4. completes within `sweep_window_ms`,
//!
//! is classified as a sweep **candidate** with an evidence-backed confidence
//! score in [0, 1]. The underlying evidence (`levels_crossed`, `volume`,
//! `duration`, `price_displacement`) is always exposed.
//!
//! ## Confidence formula
//!
//! ```text
//! level_score     = min(1, levels_crossed / sweep_min_levels)
//! volume_score    = min(1, volume / sweep_min_volume)
//! direction_score = dominant_side_volume / total_volume
//! time_score      = 1.0 if duration <= window else window / duration
//!
//! confidence = 0.35*level_score + 0.30*volume_score + 0.20*direction_score + 0.15*time_score
//! ```

use crate::analytics::clusters::TradeCluster;
use crate::analytics::config::AnalyticsConfig;
use crate::analytics::events::{AnalyticsEvent, AnalyticsEventKind};
use crate::trades::trade::AggressorSide;

/// A sweep candidate with its evidence.
#[derive(Debug, Clone, PartialEq)]
pub struct SweepCandidate {
    pub side: AggressorSide,
    pub start_ms: u64,
    pub end_ms: u64,
    pub levels_crossed: u32,
    pub volume: u64,
    pub price_displacement_ticks: u64,
    pub first_price: u64,
    pub last_price: u64,
    pub duration_ms: u64,
    pub confidence: f64,
}

/// Sweep detector — cluster-driven.
pub struct SweepDetector {
    cfg: AnalyticsConfig,
    pub count: u64,
}

impl SweepDetector {
    pub fn new(cfg: &AnalyticsConfig) -> Self {
        Self {
            cfg: cfg.clone(),
            count: 0,
        }
    }

    /// Evaluate a closed cluster. Returns a sweep-candidate event when the
    /// evidence meets the configured criteria.
    pub fn on_cluster(&mut self, cluster: &TradeCluster) -> Option<AnalyticsEvent> {
        let cfg = &self.cfg;
        let duration = cluster.duration_ms();

        // 1. Time.
        if duration > cfg.sweep_window_ms {
            return None;
        }
        // 2. Levels crossed.
        if cluster.levels_crossed < cfg.sweep_min_levels {
            return None;
        }
        // 3. Directional progression.
        if !cluster.monotonic {
            return None;
        }
        // 4. Meaningful volume.
        if cluster.total_volume < cfg.sweep_min_volume_ticks {
            return None;
        }
        // 5. Side consistency.
        if cluster.aggressor_dominance < 0.5 {
            return None;
        }

        let side = cluster.dominant_side;
        let displacement = if side == AggressorSide::Buy {
            cluster.last_price.saturating_sub(cluster.first_price)
        } else {
            cluster.first_price.saturating_sub(cluster.last_price)
        };

        let level_score = AnalyticsConfig::clamp_score(
            cluster.levels_crossed as f64 / cfg.sweep_min_levels as f64,
        );
        let volume_score = AnalyticsConfig::clamp_score(
            cluster.total_volume as f64 / cfg.sweep_min_volume_ticks as f64,
        );
        let direction_score = cluster.aggressor_dominance;
        let time_score = if duration == 0 {
            1.0
        } else {
            AnalyticsConfig::clamp_score(cfg.sweep_window_ms as f64 / duration as f64)
        };
        let confidence =
            0.35 * level_score + 0.30 * volume_score + 0.20 * direction_score + 0.15 * time_score;

        if confidence < cfg.confidence_threshold {
            return None;
        }

        self.count += 1;
        Some(
            AnalyticsEvent::new(
                AnalyticsEventKind::SweepCandidate,
                cluster.end_ms,
                "BTCUSDT",
            )
            .with_side(side.label())
            .with_price(cluster.last_price)
            .with_quantity(cluster.total_volume)
            .with_detail(serde_json::json!({
                "confidence": confidence,
                "levels_crossed": cluster.levels_crossed,
                "volume_ticks": cluster.total_volume,
                "duration_ms": duration,
                "price_displacement_ticks": displacement,
                "trade_count": cluster.trade_count,
                "start_ms": cluster.start_ms,
                "end_ms": cluster.end_ms,
                "first_price": cluster.first_price,
                "last_price": cluster.last_price,
                "buy_volume": cluster.buy_volume,
                "sell_volume": cluster.sell_volume,
                "delta": cluster.delta,
            })),
        )
    }

    pub fn candidate(&self, cluster: &TradeCluster) -> Option<SweepCandidate> {
        // Evidence-only accessor (no side effects) used by tests.
        let cfg = &self.cfg;
        let duration = cluster.duration_ms();
        if duration > cfg.sweep_window_ms
            || cluster.levels_crossed < cfg.sweep_min_levels
            || !cluster.monotonic
            || cluster.total_volume < cfg.sweep_min_volume_ticks
            || cluster.aggressor_dominance < 0.5
        {
            return None;
        }
        let side = cluster.dominant_side;
        let displacement = if side == AggressorSide::Buy {
            cluster.last_price.saturating_sub(cluster.first_price)
        } else {
            cluster.first_price.saturating_sub(cluster.last_price)
        };
        let level_score = AnalyticsConfig::clamp_score(
            cluster.levels_crossed as f64 / cfg.sweep_min_levels as f64,
        );
        let volume_score = AnalyticsConfig::clamp_score(
            cluster.total_volume as f64 / cfg.sweep_min_volume_ticks as f64,
        );
        let direction_score = cluster.aggressor_dominance;
        let time_score = if duration == 0 {
            1.0
        } else {
            AnalyticsConfig::clamp_score(cfg.sweep_window_ms as f64 / duration as f64)
        };
        let confidence =
            0.35 * level_score + 0.30 * volume_score + 0.20 * direction_score + 0.15 * time_score;
        if confidence < cfg.confidence_threshold {
            return None;
        }
        Some(SweepCandidate {
            side,
            start_ms: cluster.start_ms,
            end_ms: cluster.end_ms,
            levels_crossed: cluster.levels_crossed,
            volume: cluster.total_volume,
            price_displacement_ticks: displacement,
            first_price: cluster.first_price,
            last_price: cluster.last_price,
            duration_ms: duration,
            confidence,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_cluster(
        side: AggressorSide,
        prices: &[u64],
        volume_each: u64,
        start: u64,
        step_ms: u64,
    ) -> TradeCluster {
        let mut c = TradeCluster {
            start_ms: start,
            end_ms: start,
            high: *prices.first().unwrap(),
            low: *prices.first().unwrap(),
            total_volume: 0,
            buy_volume: 0,
            sell_volume: 0,
            delta: 0,
            trade_count: 0,
            largest_trade_quantity: volume_each,
            dominant_side: side,
            aggressor_dominance: 0.0,
            levels_crossed: 1,
            monotonic: true,
            first_price: prices[0],
            last_price: prices[0],
        };
        let mut prev = prices[0];
        for (i, p) in prices.iter().enumerate() {
            c.end_ms = start + i as u64 * step_ms;
            c.total_volume += volume_each;
            c.trade_count += 1;
            if *p != prev {
                c.levels_crossed += 1;
            }
            match side {
                AggressorSide::Buy => c.buy_volume += volume_each,
                AggressorSide::Sell => c.sell_volume += volume_each,
            }
            c.high = c.high.max(*p);
            c.low = c.low.min(*p);
            prev = *p;
        }
        c.last_price = *prices.last().unwrap();
        c.aggressor_dominance = 1.0;
        c.delta = match side {
            AggressorSide::Buy => c.total_volume as i128,
            AggressorSide::Sell => -(c.total_volume as i128),
        };
        c
    }

    #[test]
    fn test_buy_sweep_detected() {
        let cfg = AnalyticsConfig::btcusdt_default();
        let mut det = SweepDetector::new(&cfg);
        // 4 consecutive levels, 2 BTC each (8 BTC total ≥ 5 BTC min), within 100ms.
        let cluster = make_cluster(
            AggressorSide::Buy,
            &[
                68_000_000_000_000,
                68_000_100_000_000,
                68_000_200_000_000,
                68_000_300_000_000,
            ],
            200_000_000,
            1000,
            20,
        );
        let cand = det.candidate(&cluster).expect("buy sweep");
        assert_eq!(cand.levels_crossed, 4);
        assert!(cand.confidence >= 0.5);
        assert_eq!(
            det.on_cluster(&cluster).unwrap().kind,
            AnalyticsEventKind::SweepCandidate
        );
    }

    #[test]
    fn test_same_price_not_sweep() {
        let cfg = AnalyticsConfig::btcusdt_default();
        let det = SweepDetector::new(&cfg);
        let cluster = make_cluster(
            AggressorSide::Buy,
            &[68_000_000_000_000, 68_000_000_000_000, 68_000_000_000_000],
            500_000_000,
            1000,
            10,
        );
        assert!(det.candidate(&cluster).is_none());
    }

    #[test]
    fn test_sell_sweep_detected() {
        let cfg = AnalyticsConfig::btcusdt_default();
        let det = SweepDetector::new(&cfg);
        let cluster = make_cluster(
            AggressorSide::Sell,
            &[68_000_300_000_000, 68_000_200_000_000, 68_000_100_000_000],
            300_000_000,
            1000,
            20,
        );
        let cand = det.candidate(&cluster).expect("sell sweep");
        assert_eq!(cand.side, AggressorSide::Sell);
        assert_eq!(cand.levels_crossed, 3);
    }
}

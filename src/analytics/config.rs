//! Phase 4 analytics configuration.
//!
//! All thresholds are configurable. Defaults target the Binance USDⓈ-M
//! Futures BTCUSDT perpetual contract (tick size 0.10, step size 0.001 BTC)
//! and can be overridden from the CLI.

use crate::orderbook::level::{price_str_to_ticks, quantity_str_to_ticks, TICK_SCALE};

/// The analytics algorithm version. Every derived record written to storage
/// carries this tag so a later algorithm change (e.g. `phase4-v2`) never
/// silently overwrites `phase4-v1` results.
pub const DEFAULT_ANALYTICS_VERSION: &str = "phase4-v1";

/// Default aggregation intervals for time-bucketed volume.
pub const DEFAULT_AGGREGATION_INTERVALS_MS: [u64; 4] = [100, 1_000, 5_000, 60_000];

/// BTCUSDT perpetual tick size (0.10 USDT).
pub const DEFAULT_TICK_SIZE_STR: &str = "0.10";

/// Default retention window for in-memory analytics state (15 minutes).
pub const DEFAULT_RETENTION_MS: u64 = 900_000;

/// Convert a BTC quantity string to integer ticks (1e8 scale).
pub fn btc_str_to_ticks(s: &str) -> anyhow::Result<u64> {
    quantity_str_to_ticks(s)
}

/// Convert a nominal BTC float into integer ticks. Deterministic rounding.
pub fn btc_to_ticks(btc: f64) -> u64 {
    (btc * TICK_SCALE as f64).round().max(0.0) as u64
}

#[derive(Debug, Clone)]
pub struct AnalyticsConfig {
    /// Analytics algorithm version tag (e.g. "phase4-v1").
    pub analytics_version: String,
    /// Number of integer ticks per exchange tick size (BTCUSDT: 10_000_000).
    pub tick_size_ticks: u64,
    /// Absolute quantity threshold (in ticks) for large-trade detection.
    pub large_trade_min_quantity_ticks: u64,
    /// Sweep detection window (ms).
    pub sweep_window_ms: u64,
    /// Minimum distinct price levels crossed for a sweep candidate.
    pub sweep_min_levels: u32,
    /// Minimum aggregate volume (ticks) for a sweep candidate.
    pub sweep_min_volume_ticks: u64,
    /// Absorption detection window (ms).
    pub absorption_window_ms: u64,
    /// Minimum aggressive volume (ticks) for an absorption candidate.
    pub absorption_min_volume_ticks: u64,
    /// Minimum number of aggressive trades for an absorption candidate.
    pub absorption_min_trades: u32,
    /// Maximum favorable price excursion (ticks) tolerated for absorption.
    pub absorption_max_excursion_ticks: u64,
    /// Replenishment detection window (ms): a decrease followed by an
    /// increase at the same level within this window is flagged.
    pub replenishment_window_ms: u64,
    /// Number of best levels scanned for book imbalance and depth volumes.
    pub imbalance_depth: u32,
    /// Interval (ms) at which `MarketMicrostructureSnapshot`s are produced.
    pub snapshot_interval_ms: u64,
    /// Aggregation intervals (ms) for time-bucketed volume.
    pub aggregation_intervals_ms: Vec<u64>,
    /// Heatmap cell width (ms).
    pub heatmap_cell_ms: u64,
    /// In-memory retention window (ms) for time-based analytics state.
    pub retention_ms: u64,
    /// Trade-cluster time window (ms).
    pub cluster_window_ms: u64,
    /// Trade-cluster price range (ticks).
    pub cluster_price_range_ticks: u64,
    /// Minimum confidence (0..1) to emit a sweep/absorption candidate.
    pub confidence_threshold: f64,
    /// Heatmap price aggregation factor: how many base ticks per grid tick.
    /// Default 1 = 1 exchange tick per cell. Supported: 1, 5, 10, 25, 50.
    pub heatmap_price_aggregation: u64,
    /// Available time bucket intervals for the heatmap (ms).
    /// Default includes: 100, 250, 500, 1s, 2s, 5s, 10s, 30s, 1m.
    pub heatmap_time_intervals: Vec<u64>,
    /// Maximum number of distinct price levels the heatmap will track.
    /// When exceeded, oldest-price levels are pruned. 0 = unlimited.
    pub max_heatmap_price_levels: usize,
}

impl AnalyticsConfig {
    /// Build a default configuration for the BTCUSDT perpetual.
    pub fn btcusdt_default() -> Self {
        Self {
            analytics_version: DEFAULT_ANALYTICS_VERSION.to_string(),
            tick_size_ticks: price_str_to_ticks(DEFAULT_TICK_SIZE_STR).unwrap_or(TICK_SCALE / 10),
            large_trade_min_quantity_ticks: btc_to_ticks(5.0),
            sweep_window_ms: 100,
            sweep_min_levels: 3,
            sweep_min_volume_ticks: btc_to_ticks(5.0),
            absorption_window_ms: 1_000,
            absorption_min_volume_ticks: btc_to_ticks(20.0),
            absorption_min_trades: 5,
            absorption_max_excursion_ticks: 3,
            replenishment_window_ms: 250,
            imbalance_depth: 10,
            snapshot_interval_ms: 1_000,
            aggregation_intervals_ms: DEFAULT_AGGREGATION_INTERVALS_MS.to_vec(),
            heatmap_cell_ms: 1_000,
            retention_ms: DEFAULT_RETENTION_MS,
            cluster_window_ms: 100,
            cluster_price_range_ticks: 3,
            confidence_threshold: 0.5,
            heatmap_price_aggregation: 1,
            heatmap_time_intervals: vec![
                100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 60_000,
            ],
            max_heatmap_price_levels: 10_000,
        }
    }

    /// Confidence (0..1) from an evidence ratio, clamped.
    pub fn clamp_score(value: f64) -> f64 {
        value.clamp(0.0, 1.0)
    }

    /// The configured absorption max excursion expressed in raw price ticks
    /// (1e8 scale). The CLI value is in tick-size units (e.g. 3 × 0.10).
    pub fn absorption_max_excursion_raw(&self) -> u64 {
        self.absorption_max_excursion_ticks
            .saturating_mul(self.tick_size_ticks)
    }

    /// The configured cluster price range expressed in raw price ticks.
    pub fn cluster_price_range_raw(&self) -> u64 {
        self.cluster_price_range_ticks
            .saturating_mul(self.tick_size_ticks)
    }

    /// The price aggregation factor for the heatmap (1, 5, 10, 25, or 50).
    /// Default 1 means 1 exchange tick per cell.
    pub fn heatmap_price_aggregation_raw(&self) -> u64 {
        self.heatmap_price_aggregation
    }

    /// The configured time bucket intervals for the heatmap.
    pub fn heatmap_time_intervals(&self) -> &Vec<u64> {
        &self.heatmap_time_intervals
    }
}

impl Default for AnalyticsConfig {
    fn default() -> Self {
        Self::btcusdt_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults_are_sane() {
        let cfg = AnalyticsConfig::default();
        assert_eq!(cfg.tick_size_ticks, 10_000_000);
        assert_eq!(cfg.large_trade_min_quantity_ticks, 500_000_000);
        assert_eq!(cfg.imbalance_depth, 10);
        assert_eq!(
            cfg.aggregation_intervals_ms,
            vec![100, 1_000, 5_000, 60_000]
        );
    }

    #[test]
    fn test_btc_to_ticks_exact() {
        assert_eq!(btc_to_ticks(1.0), 100_000_000);
        assert_eq!(btc_to_ticks(5.0), 500_000_000);
        assert_eq!(btc_to_ticks(0.0), 0);
    }

    #[test]
    fn test_clamp_score() {
        assert_eq!(AnalyticsConfig::clamp_score(-0.5), 0.0);
        assert_eq!(AnalyticsConfig::clamp_score(0.7), 0.7);
        assert_eq!(AnalyticsConfig::clamp_score(1.5), 1.0);
    }

    #[test]
    fn test_heatmap_default_aggregation_is_1() {
        let cfg = AnalyticsConfig::btcusdt_default();
        assert_eq!(cfg.heatmap_price_aggregation, 1);
        assert_eq!(
            cfg.heatmap_time_intervals,
            vec![100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 60_000]
        );
    }
}

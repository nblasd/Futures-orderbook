//! `MarketMicrostructureSnapshot` — the main analytical output of the engine.
//!
//! Produced at every `snapshot_interval_ms` boundary. Volume/delta/liquidity
//! counters are **interval** values (since the previous snapshot) so the
//! snapshot stream can be queried for "delta over time"; `cvd` is the running
//! **session** value at the snapshot timestamp.

use crate::orderbook::level::{ticks_to_price_str, ticks_to_quantity_str, TICK_SCALE};

#[derive(Debug, Clone, PartialEq)]
pub struct MarketMicrostructureSnapshot {
    pub symbol: String,
    pub timestamp_ms: u64,
    pub analytics_version: String,

    // --- Book state ---
    pub book_ready: bool,
    pub best_bid: Option<u64>,
    pub best_ask: Option<u64>,
    pub mid_price: Option<f64>,
    pub spread_ticks: Option<u64>,
    /// Microprice as an exact rational (numerator / denominator, ticks).
    pub microprice_num: Option<u128>,
    pub microprice_den: Option<u128>,

    // --- Flow (interval since last snapshot, except cvd) ---
    pub trade_volume: u64,
    pub buy_volume: u64,
    pub sell_volume: u64,
    pub delta: i128,
    /// Running session CVD at snapshot time.
    pub cvd: i128,

    // --- Book depth (interval-end state) ---
    pub bid_depth: u64,
    pub ask_depth: u64,
    pub book_imbalance: Option<f64>,

    // --- Liquidity (interval) ---
    pub liquidity_added: u64,
    pub liquidity_removed: u64,

    // --- Derived-event counts (interval) ---
    pub large_trade_count: u64,
    pub sweep_candidate_count: u64,
    pub absorption_candidate_count: u64,
    pub replenishment_count: u64,

    // --- Data quality ---
    pub book_crossed: bool,
    pub anomalies: u64,
}

impl MarketMicrostructureSnapshot {
    pub fn microprice_f64(&self) -> Option<f64> {
        let n = self.microprice_num?;
        let d = self.microprice_den?;
        if d == 0 {
            return None;
        }
        Some(n as f64 / d as f64 / TICK_SCALE as f64)
    }
}

/// A condensed, deterministic fingerprint of the flow analytics used to
/// compare two runs (live vs replay) for consistency.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AnalyticsFlowDigest {
    pub trade_count: u64,
    pub trade_volume: u64,
    pub buy_volume: u64,
    pub sell_volume: u64,
    pub delta: i128,
    pub cvd: i128,
    pub large_trade_count: u64,
    pub sweep_candidate_count: u64,
    pub absorption_candidate_count: u64,
    pub replenishment_count: u64,
    pub liquidity_added: u64,
    pub liquidity_removed: u64,
    /// Sorted list of (price, total_volume, buy, sell, delta) for all prices.
    pub volume_by_price: Vec<(u64, u64, u64, u64, i128)>,
}

impl AnalyticsFlowDigest {
    pub fn summarize(&self) -> String {
        format!(
            "trades={} volume={} buy={} sell={} delta={} cvd={} large={} sweeps={} absorption={} replenishments={} liq_added={} liq_removed={} prices={}",
            self.trade_count,
            ticks_to_quantity_str(self.trade_volume),
            ticks_to_quantity_str(self.buy_volume),
            ticks_to_quantity_str(self.sell_volume),
            self.delta,
            self.cvd,
            self.large_trade_count,
            self.sweep_candidate_count,
            self.absorption_candidate_count,
            self.replenishment_count,
            ticks_to_quantity_str(self.liquidity_added),
            ticks_to_quantity_str(self.liquidity_removed),
            self.volume_by_price.len(),
        )
    }
}

/// Format a price tick for a snapshot field.
pub fn fmt_price(ticks: u64) -> String {
    ticks_to_price_str(ticks)
}

//! Price × Time heatmap data model — Phase 5.
//!
//! The heatmap tracks, per price level and time bucket, the trade flow
//! (volume, buy/sell split), resting visible order-book liquidity, and
//! derived analytics signals (sweeps, absorption, replenishment). It is a
//! bounded in-memory model with a configurable retention window; the
//! per-cell aggregation is emitted as part of the analytics snapshot.
//!
//! ## Public API
//!
//! * [`Heatmap::on_trade`] — feed an aggressive trade.
//! * [`Heatmap::on_level_change`] — feed a resting-liquidity level change.
//! * [`Heatmap::on_large_trade`] — record large-trade volume at a price.
//! * [`Heatmap::on_sweep`] — map a sweep candidate event into a cell.
//! * [`Heatmap::on_absorption`] — map an absorption candidate event into a cell.
//! * [`Heatmap::on_replenishment`] — map a replenishment event into a cell.
//! * [`HeatmapFrame::from_heatmap`] — renderer-friendly snapshot.
//! * [`Heatmap::digest`] — deterministic fingerprint for live/replay comparison.
//!
//! ## Persistence
//!
//! This module produces **derived** data. Raw market events remain the
//! authoritative source. Heatmap records can be persisted as JSON snapshots
//! or compact summaries, but loss of derived records is always recoverable
//! by replaying raw events through the engine.
//!
//! No drawing or charting is produced here — this module owns the **data
//! model** only.

use std::collections::BTreeMap;

use crate::analytics::book::{BookSide, LevelChange};
use crate::analytics::config::AnalyticsConfig;
use crate::trades::trade::{AggressorSide, TradeEvent};

/// One heatmap cell: flow + liquidity at a price within a time bucket.
///
/// All values are integer representations. The cell combines executed
// trade flow data with resting visible liquidity footprints derived from
// the order-book stream via `Heatmap::on_level_change`.
///
/// ## Determinism
///
/// * All fields are `u64`/`i128` — no floating point.
/// * Iteration order is deterministic via `BTreeMap`.
/// * Bucket assignment uses exchange event timestamps only (never local time).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HeatmapCell {
    /// Price tick (raw 1e8 scale; grid-aligned via price_aggregation in the heatmap).
    pub price: u64,
    /// Resting visible bid liquidity at this price within the bucket.
    pub resting_bid_liquidity: u64,
    /// Resting visible ask liquidity at this price within the bucket.
    pub resting_ask_liquidity: u64,
    /// Cumulative liquidity added at this price (across both sides).
    pub liquidity_added: u64,
    /// Cumulative liquidity removed at this price (across both sides).
    pub liquidity_removed: u64,
    /// Executed buy volume (aggressive buys at this price within the bucket).
    pub executed_buy_volume: u64,
    /// Executed sell volume (aggressive sells at this price within the bucket).
    pub executed_sell_volume: u64,
    /// Delta = bought_volume - sold_volume (aggressor sign convention).
    pub delta: i128,
    /// Number of trades at this price within the bucket.
    pub trade_count: u64,
    /// Volume from large-trade candidates at this price.
    pub large_trade_volume: u64,
    /// Number of replenishment candidates at this price.
    pub replenishment_count: u64,
    /// Number of absorption candidates at this price.
    pub absorption_candidate_count: u64,
    /// Number of sweep candidates at this price.
    pub sweep_count: u64,
    /// Net buy pressure (buy - sell) at this price; running aggregate.
    pub pressure: i128,
    /// The time bucket start (ms since epoch) this cell belongs to.
    /// `None` when the cell has not been assigned to a bucket yet
    /// (e.g. before any event has been fed).
    pub timestamp_bucket: Option<u64>,
}

impl HeatmapCell {
    /// Create a new cell at the given price tick.
    pub fn new(price: u64) -> Self {
        Self {
            price,
            timestamp_bucket: None,
            ..Default::default()
        }
    }

    /// Total executed volume (buy + sell).
    pub fn total_executed_volume(&self) -> u64 {
        self.executed_buy_volume + self.executed_sell_volume
    }

    /// Imbalance ratio in [-1, 1]: (buy - sell) / total, or 0 if no volume.
    pub fn imbalance_f64(&self) -> f64 {
        let total = self.total_executed_volume() as f64;
        if total == 0.0 {
            0.0
        } else {
            self.delta as f64 / total
        }
    }

    /// Whether this cell has any executed trades.
    pub fn has_executed_volume(&self) -> bool {
        self.trade_count > 0
    }

    /// Whether this cell has any resting liquidity on either side.
    pub fn has_resting_liquidity(&self) -> bool {
        self.resting_bid_liquidity > 0 || self.resting_ask_liquidity > 0
    }

    // ------------------------------------------------------------------
    // Intensity methods — deterministic, normalized numeric fields.
    // The renderer decides the visual palette; these produce values
    // suitable for direct use as colour-intensity scalars.
    // ------------------------------------------------------------------

    /// Liquidity intensity in [0, 1]: combined resting bid+ask as a
    /// fraction of `max_liquidity` (the peak value across all cells).
    /// Returns 0.0 if `max_liquidity` is 0.
    pub fn liquidity_intensity(&self, max_liquidity: u64) -> f64 {
        if max_liquidity == 0 {
            0.0
        } else {
            let total = self.resting_bid_liquidity + self.resting_ask_liquidity;
            (total as f64 / max_liquidity as f64).clamp(0.0, 1.0)
        }
    }

    /// Execution intensity in [0, 1]: total executed volume as a
    /// fraction of `max_volume`.
    pub fn execution_intensity(&self, max_volume: u64) -> f64 {
        if max_volume == 0 {
            0.0
        } else {
            (self.total_executed_volume() as f64 / max_volume as f64).clamp(0.0, 1.0)
        }
    }

    /// Delta intensity in [0, 1]: absolute delta normalised by
    /// `max_abs_delta`. Useful for colouring buy-vs-sell imbalance.
    pub fn delta_intensity(&self, max_abs_delta: i128) -> f64 {
        if max_abs_delta == 0 {
            0.0
        } else {
            (self.delta.unsigned_abs() as f64 / max_abs_delta as f64).clamp(0.0, 1.0)
        }
    }

    /// Absorption intensity in [0, 1]: `absorption_candidate_count`
    /// normalised by `max_absorption_count`.
    pub fn absorption_intensity(&self, max_absorption_count: u64) -> f64 {
        if max_absorption_count == 0 {
            0.0
        } else {
            (self.absorption_candidate_count as f64 / max_absorption_count as f64).clamp(0.0, 1.0)
        }
    }

    /// Sweep intensity in [0, 1]: `sweep_count` normalised by
    /// `max_sweep_count`.
    pub fn sweep_intensity(&self, max_sweep_count: u64) -> f64 {
        if max_sweep_count == 0 {
            0.0
        } else {
            (self.sweep_count as f64 / max_sweep_count as f64).clamp(0.0, 1.0)
        }
    }
}

/// The heatmap: time bucket (ms since epoch) → (price grid tick → cell).
///
/// Bounded in-memory model with configurable retention. The price grid
/// supports aggregation resolutions (1×, 5×, 10×, 25×, 50× the base tick)
/// and the time grid supports configurable bucket intervals
/// (100ms, 250ms, 500ms, 1s, 2s, 5s, 10s, 30s, 1m).
///
/// The heatmap must be fed through the existing `MarketEvent` pipeline
/// (trades via `on_trade`, level changes via `on_level_change`). It does
/// not ingest directly from Binance.
pub struct Heatmap {
    /// Bucket start (ms since epoch) → price grid tick → cell.
    buckets: BTreeMap<u64, BTreeMap<u64, HeatmapCell>>,
    /// Cell width (ms) determines the default time bucket size.
    cell_ms: u64,
    /// Retention window (ms): buckets older than this are pruned.
    retention_ms: u64,
    /// Price aggregation factor: how many base ticks per grid tick.
    /// Default 1 = 1 exchange tick per cell. Supported: 1, 5, 10, 25, 50.
    price_aggregation: u64,
    /// Available time bucket intervals (ms). The cell_ms determines the
    /// default bucket width used by on_trade / on_level_change.
    time_intervals: Vec<u64>,
}

impl Heatmap {
    /// Create a new heatmap from the configured settings.
    pub fn new(cfg: &AnalyticsConfig) -> Self {
        let default_intervals = [100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 60_000];
        Self {
            buckets: BTreeMap::new(),
            cell_ms: cfg.heatmap_cell_ms,
            retention_ms: cfg.retention_ms,
            price_aggregation: 1,
            time_intervals: default_intervals.to_vec(),
        }
    }

    /// Set the price aggregation factor. Supported values: 1, 5, 10, 25, 50.
    /// When set to N, every N exchange ticks are aggregated into one cell price.
    pub fn set_price_aggregation(&mut self, aggregation: u64) {
        match aggregation {
            1 | 5 | 10 | 25 | 50 => self.price_aggregation = aggregation,
            _ => self.price_aggregation = 1, // fallback to 1
        }
    }

    /// Set the available time bucket intervals (ms).
    pub fn set_time_intervals(&mut self, intervals: Vec<u64>) {
        self.time_intervals = intervals;
    }

    /// Get the grid-aligned price tick for a given raw price tick.
    fn grid_price(&self, price_ticks: u64) -> u64 {
        let agg = self.price_aggregation;
        if agg == 1 {
            price_ticks
        } else {
            price_ticks / agg
        }
    }

    /// Get the time bucket start (ms) for a given timestamp and interval.
    fn bucket_start(&self, timestamp_ms: u64, interval_ms: u64) -> u64 {
        timestamp_ms - (timestamp_ms % interval_ms)
    }

    /// Feed a trade into the heatmap.
    ///
    /// Updates executed volume, delta, trade count, and pressure at the
    /// price × time-bucket cell. Uses the configured `cell_ms` for bucket
    /// width. Source event timestamp is authoritative for bucketing.
    pub fn on_trade(&mut self, trade: &TradeEvent) {
        let ts = trade.trade_time;
        let bucket_start = ts - (ts % self.cell_ms);
        let price = self.grid_price(trade.price_ticks);
        let cell = self
            .buckets
            .entry(bucket_start)
            .or_default()
            .entry(price)
            .or_insert_with(|| HeatmapCell::new(price));

        cell.timestamp_bucket = Some(bucket_start);
        cell.price = price;
        cell.trade_count += 1;
        cell.executed_buy_volume += match trade.aggressor {
            AggressorSide::Buy => trade.quantity_ticks,
            AggressorSide::Sell => 0,
        };
        cell.executed_sell_volume += match trade.aggressor {
            AggressorSide::Sell => trade.quantity_ticks,
            AggressorSide::Buy => 0,
        };
        cell.delta += match trade.aggressor {
            AggressorSide::Buy => trade.quantity_ticks as i128,
            AggressorSide::Sell => -(trade.quantity_ticks as i128),
        };
        cell.pressure += match trade.aggressor {
            AggressorSide::Buy => trade.quantity_ticks as i128,
            AggressorSide::Sell => -(trade.quantity_ticks as i128),
        };
    }

    /// Record a large trade at the given price/quantity.
    /// Updates the `large_trade_volume` field in the corresponding cell.
    pub fn on_large_trade(&mut self, trade: &TradeEvent) {
        let ts = trade.trade_time;
        let bucket_start = ts - (ts % self.cell_ms);
        let price = self.grid_price(trade.price_ticks);
        let cell = self
            .buckets
            .entry(bucket_start)
            .or_default()
            .entry(price)
            .or_insert_with(|| HeatmapCell::new(price));

        cell.timestamp_bucket = Some(bucket_start);
        cell.price = price;
        cell.large_trade_volume += trade.quantity_ticks;
    }

    /// Feed a level change into the heatmap (for resting liquidity tracking).
    ///
    /// Derived from `OrderBookUpdated` events via the shadow book's
    /// `LevelChange`. Updates resting bid/ask liquidity and cumulative
    /// added/removed quantities. Uses the configured `cell_ms` for bucket
    /// width. Source event timestamp is authoritative for bucketing.
    pub fn on_level_change(&mut self, change: &LevelChange, ts: u64) {
        let bucket_start = ts - (ts % self.cell_ms);
        let price = self.grid_price(change.price);
        let cell = self
            .buckets
            .entry(bucket_start)
            .or_default()
            .entry(price)
            .or_insert_with(|| HeatmapCell::new(price));

        cell.timestamp_bucket = Some(bucket_start);
        cell.price = price;

        match change.side {
            BookSide::Bid => {
                let old = cell.resting_bid_liquidity;
                let new = change.new_qty;
                if new > old {
                    cell.liquidity_added += new - old;
                    cell.resting_bid_liquidity = new;
                } else if new < old {
                    let removed = old.saturating_sub(new);
                    cell.liquidity_removed += removed;
                    cell.resting_bid_liquidity = new;
                }
            }
            BookSide::Ask => {
                let old = cell.resting_ask_liquidity;
                let new = change.new_qty;
                if new > old {
                    cell.liquidity_added += new - old;
                    cell.resting_ask_liquidity = new;
                } else if new < old {
                    let removed = old.saturating_sub(new);
                    cell.liquidity_removed += removed;
                    cell.resting_ask_liquidity = new;
                }
            }
        }
    }

    /// Iterate over buckets (oldest first) with their price→cell maps.
    pub fn buckets(&self) -> impl Iterator<Item = (u64, &BTreeMap<u64, HeatmapCell>)> {
        self.buckets.iter().map(|(ts, cells)| (*ts, cells))
    }

    /// Total executed buy/sell/total/delta in a price range [lo, hi] within
    /// the last `window_ms` milliseconds.
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
                buy += cell.executed_buy_volume;
                sell += cell.executed_sell_volume;
                total += cell.total_executed_volume();
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

    /// Record a sweep candidate event at the given price and timestamp.
    /// Increments the `sweep_count` in the corresponding cell.
    pub fn on_sweep(&mut self, price_ticks: u64, ts: u64) {
        let bucket_start = ts - (ts % self.cell_ms);
        let price = self.grid_price(price_ticks);
        let cell = self
            .buckets
            .entry(bucket_start)
            .or_default()
            .entry(price)
            .or_insert_with(|| HeatmapCell::new(price));
        cell.timestamp_bucket = Some(bucket_start);
        cell.price = price;
        cell.sweep_count += 1;
    }

    /// Record an absorption candidate event at the given price and timestamp.
    /// Increments the `absorption_candidate_count` in the corresponding cell.
    pub fn on_absorption(&mut self, price_ticks: u64, ts: u64) {
        let bucket_start = ts - (ts % self.cell_ms);
        let price = self.grid_price(price_ticks);
        let cell = self
            .buckets
            .entry(bucket_start)
            .or_default()
            .entry(price)
            .or_insert_with(|| HeatmapCell::new(price));
        cell.timestamp_bucket = Some(bucket_start);
        cell.price = price;
        cell.absorption_candidate_count += 1;
    }

    /// Record a replenishment event at the given price and timestamp.
    /// Increments the `replenishment_count` in the corresponding cell.
    pub fn on_replenishment(&mut self, price_ticks: u64, ts: u64) {
        let bucket_start = ts - (ts % self.cell_ms);
        let price = self.grid_price(price_ticks);
        let cell = self
            .buckets
            .entry(bucket_start)
            .or_default()
            .entry(price)
            .or_insert_with(|| HeatmapCell::new(price));
        cell.timestamp_bucket = Some(bucket_start);
        cell.price = price;
        cell.replenishment_count += 1;
    }

    /// Compute a deterministic digest of the heatmap state for live/replay
    /// comparison. The digest is order-independent (uses sorted BTreeMap
    /// iteration) and reproducible for identical event streams.
    pub fn digest(&self) -> HeatmapDigest {
        let mut total_buckets = 0usize;
        let mut total_price_levels = 0usize;
        let mut total_executed_buy = 0u64;
        let mut total_executed_sell = 0u64;
        let mut total_delta = 0i128;
        let mut total_trade_count = 0u64;
        let mut total_liquidity_added = 0u64;
        let mut total_liquidity_removed = 0u64;
        let mut total_resting_bid = 0u64;
        let mut total_resting_ask = 0u64;
        let mut total_large_trade_volume = 0u64;
        let mut total_replenishment_count = 0u64;
        let mut total_absorption_candidate_count = 0u64;
        let mut total_sweep_count = 0u64;
        let mut total_pressure = 0i128;

        for cells_map in self.buckets.values() {
            total_buckets += 1;
            for cell in cells_map.values() {
                total_price_levels += 1;
                total_executed_buy += cell.executed_buy_volume;
                total_executed_sell += cell.executed_sell_volume;
                total_delta += cell.delta;
                total_trade_count += cell.trade_count;
                total_liquidity_added += cell.liquidity_added;
                total_liquidity_removed += cell.liquidity_removed;
                total_resting_bid += cell.resting_bid_liquidity;
                total_resting_ask += cell.resting_ask_liquidity;
                total_large_trade_volume += cell.large_trade_volume;
                total_replenishment_count += cell.replenishment_count;
                total_absorption_candidate_count += cell.absorption_candidate_count;
                total_sweep_count += cell.sweep_count;
                total_pressure += cell.pressure;
            }
        }

        HeatmapDigest {
            total_buckets,
            total_price_levels,
            total_executed_buy,
            total_executed_sell,
            total_delta,
            total_trade_count,
            total_liquidity_added,
            total_liquidity_removed,
            total_resting_bid,
            total_resting_ask,
            total_large_trade_volume,
            total_replenishment_count,
            total_absorption_candidate_count,
            total_sweep_count,
            total_pressure,
        }
    }
}

/// A deterministic fingerprint of the heatmap state for live vs replay
/// comparison. All fields are integers (or derived from integers) so
/// comparison is exact when identical event streams are processed.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HeatmapDigest {
    pub total_buckets: usize,
    pub total_price_levels: usize,
    pub total_executed_buy: u64,
    pub total_executed_sell: u64,
    pub total_delta: i128,
    pub total_trade_count: u64,
    pub total_liquidity_added: u64,
    pub total_liquidity_removed: u64,
    pub total_resting_bid: u64,
    pub total_resting_ask: u64,
    pub total_large_trade_volume: u64,
    pub total_replenishment_count: u64,
    pub total_absorption_candidate_count: u64,
    pub total_sweep_count: u64,
    pub total_pressure: i128,
}

impl HeatmapDigest {
    pub fn summarize(&self) -> String {
        format!(
            "buckets={} price_levels={} buy={} sell={} delta={} trades={} liq_added={} liq_removed={} resting_bid={} resting_ask={} large_vol={} replenish={} absorb={} sweeps={} pressure={}",
            self.total_buckets,
            self.total_price_levels,
            self.total_executed_buy,
            self.total_executed_sell,
            self.total_delta,
            self.total_trade_count,
            self.total_liquidity_added,
            self.total_liquidity_removed,
            self.total_resting_bid,
            self.total_resting_ask,
            self.total_large_trade_volume,
            self.total_replenishment_count,
            self.total_absorption_candidate_count,
            self.total_sweep_count,
            self.total_pressure,
        )
    }
}

/// A renderer-friendly snapshot of the heatmap state at a given moment.
///
/// This can be serialized (JSON) and sent to a frontend renderer. It
/// contains only the data needed for visualization — no internal
/// implementation details.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HeatmapFrame {
    /// Snapshot timestamp (ms since epoch).
    pub timestamp: u64,
    /// Visible price range (in grid ticks).
    pub visible_price_range: (u64, u64),
    /// Time range covered by the frame (start_ms, end_ms).
    pub time_range: (u64, u64),
    /// Per-price cells in the visible range.
    pub cells: Vec<HeatmapCellSnapshot>,
    /// Summary metadata.
    pub summary: HeatmapSummary,
}

/// A single cell within a `HeatmapFrame`, suitable for serialization.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HeatmapCellSnapshot {
    /// Grid-aligned price tick.
    pub price_tick: u64,
    /// Resting bid liquidity.
    pub resting_bid_liquidity: u64,
    /// Resting ask liquidity.
    pub resting_ask_liquidity: u64,
    /// Cumulative liquidity added.
    pub liquidity_added: u64,
    /// Cumulative liquidity removed.
    pub liquidity_removed: u64,
    /// Executed buy volume.
    pub executed_buy_volume: u64,
    /// Executed sell volume.
    pub executed_sell_volume: u64,
    /// Delta (buy - sell).
    pub delta: i128,
    /// Trade count.
    pub trade_count: u64,
    /// Large-trade volume.
    pub large_trade_volume: u64,
    /// Replenishment count.
    pub replenishment_count: u64,
    /// Absorption candidate count.
    pub absorption_candidate_count: u64,
    /// Sweep count.
    pub sweep_count: u64,
    /// Net pressure.
    pub pressure: i128,
}

impl HeatmapCellSnapshot {
    /// Create a snapshot from a `HeatmapCell`.
    pub fn from_cell(cell: &HeatmapCell, price: u64) -> Self {
        Self {
            price_tick: price,
            resting_bid_liquidity: cell.resting_bid_liquidity,
            resting_ask_liquidity: cell.resting_ask_liquidity,
            liquidity_added: cell.liquidity_added,
            liquidity_removed: cell.liquidity_removed,
            executed_buy_volume: cell.executed_buy_volume,
            executed_sell_volume: cell.executed_sell_volume,
            delta: cell.delta,
            trade_count: cell.trade_count,
            large_trade_volume: cell.large_trade_volume,
            replenishment_count: cell.replenishment_count,
            absorption_candidate_count: cell.absorption_candidate_count,
            sweep_count: cell.sweep_count,
            pressure: cell.pressure,
        }
    }
}

/// Summary metadata for a `HeatmapFrame`.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HeatmapSummary {
    /// Total number of price levels across all buckets.
    pub total_price_levels: usize,
    /// Total number of time buckets.
    pub total_buckets: usize,
    /// Total executed buy volume across all cells.
    pub total_executed_buy: u64,
    /// Total executed sell volume across all cells.
    pub total_executed_sell: u64,
    /// Total delta across all cells.
    pub total_delta: i128,
    /// Total trade count across all cells.
    pub total_trade_count: u64,
    /// Total liquidity added across all cells.
    pub total_liquidity_added: u64,
    /// Total liquidity removed across all cells.
    pub total_liquidity_removed: u64,
    /// Total large-trade volume across all cells.
    pub total_large_trade_volume: u64,
    /// Total replenishment count across all cells.
    pub total_replenishment_count: u64,
    /// Total absorption candidate count across all cells.
    pub total_absorption_candidate_count: u64,
    /// Total sweep count across all cells.
    pub total_sweep_count: u64,
}

impl HeatmapFrame {
    /// Create a frame from the heatmap state at the given timestamp,
    /// considering only cells within the visible price range [visible_lo, visible_hi].
    pub fn from_heatmap(
        heatmap: &Heatmap,
        timestamp: u64,
        visible_lo: u64,
        visible_hi: u64,
    ) -> Self {
        let mut cells = Vec::new();
        let mut total_buy = 0u64;
        let mut total_sell = 0u64;
        let mut total_delta = 0i128;
        let mut total_trade_count = 0u64;
        let mut total_liq_added = 0u64;
        let mut total_liq_removed = 0u64;
        let mut total_large_vol = 0u64;
        let mut total_replenish = 0u64;
        let mut total_absorp = 0u64;
        let mut total_sweeps = 0u64;
        let mut total_price_levels = 0usize;

        let mut min_price = visible_hi;
        let mut max_price = visible_lo;

        for cells_map in heatmap.buckets.values() {
            for (price, cell) in cells_map.iter() {
                if *price < visible_lo || *price > visible_hi {
                    continue;
                }
                cells.push(HeatmapCellSnapshot::from_cell(cell, *price));
                total_buy += cell.executed_buy_volume;
                total_sell += cell.executed_sell_volume;
                total_delta += cell.delta;
                total_trade_count += cell.trade_count;
                total_liq_added += cell.liquidity_added;
                total_liq_removed += cell.liquidity_removed;
                total_large_vol += cell.large_trade_volume;
                total_replenish += cell.replenishment_count;
                total_absorp += cell.absorption_candidate_count;
                total_sweeps += cell.sweep_count;
                total_price_levels += 1;
                if *price < min_price {
                    min_price = *price;
                }
                if *price > max_price {
                    max_price = *price;
                }
            }
        }

        let effective_lo = if cells.is_empty() {
            visible_lo
        } else {
            min_price
        };
        let effective_hi = if cells.is_empty() {
            visible_hi
        } else {
            max_price
        };

        // Compute time range from bucket boundaries.
        let bucket_starts: Vec<u64> = heatmap.buckets.keys().cloned().collect();
        let time_range_start = if bucket_starts.is_empty() {
            timestamp.saturating_sub(heatmap.retention_ms)
        } else {
            *bucket_starts.iter().min().unwrap()
        };
        let time_range_end = if bucket_starts.is_empty() {
            timestamp
        } else {
            *bucket_starts.iter().max().unwrap()
        };

        Self {
            timestamp,
            visible_price_range: (effective_lo, effective_hi),
            time_range: (time_range_start, time_range_end),
            cells,
            summary: HeatmapSummary {
                total_price_levels,
                total_buckets: heatmap.bucket_count(),
                total_executed_buy: total_buy,
                total_executed_sell: total_sell,
                total_delta,
                total_trade_count,
                total_liquidity_added: total_liq_added,
                total_liquidity_removed: total_liq_removed,
                total_large_trade_volume: total_large_vol,
                total_replenishment_count: total_replenish,
                total_absorption_candidate_count: total_absorp,
                total_sweep_count: total_sweeps,
            },
        }
    }
}

/// Incremental delta for the heatmap, allowing a UI renderer to update
/// incrementally rather than receiving a full snapshot every interval.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct HeatmapDelta {
    /// Cells that changed (price → new cell data).
    pub changed: Vec<(u64, HeatmapCellSnapshot)>,
    /// Cells that were newly created.
    pub new: Vec<HeatmapCellSnapshot>,
    /// Cells that were removed.
    pub removed: Vec<u64>, // price ticks
    /// Updated summary values.
    pub summary_delta: HeatmapSummaryDelta,
}

/// Delta between two summary states.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HeatmapSummaryDelta {
    /// Change in total executed buy volume.
    pub total_executed_buy: u64,
    /// Change in total executed sell volume.
    pub total_executed_sell: u64,
    /// Change in total delta.
    pub total_delta: i128,
    /// Change in total trade count.
    pub total_trade_count: u64,
    /// Change in total liquidity added.
    pub total_liquidity_added: u64,
    /// Change in total liquidity removed.
    pub total_liquidity_removed: u64,
    /// Change in total large-trade volume.
    pub total_large_trade_volume: u64,
    /// Change in total replenishment count.
    pub total_replenishment_count: u64,
    /// Change in total absorption candidate count.
    pub total_absorption_candidate_count: u64,
    /// Change in total sweep count.
    pub total_sweep_count: u64,
}

impl HeatmapDelta {
    /// Compute an incremental delta between two heatmap frames.
    /// Changed = cells present in both but with different data.
    /// New = cells in `current` not in `previous`.
    /// Removed = cells in `previous` not in `current`.
    pub fn compute(previous: &HeatmapFrame, current: &HeatmapFrame) -> Self {
        use std::collections::BTreeMap;

        let prev_map: BTreeMap<u64, &HeatmapCellSnapshot> =
            previous.cells.iter().map(|c| (c.price_tick, c)).collect();
        let curr_map: BTreeMap<u64, &HeatmapCellSnapshot> =
            current.cells.iter().map(|c| (c.price_tick, c)).collect();

        let mut changed = Vec::new();
        let mut new_cells = Vec::new();
        let mut removed = Vec::new();

        for (price, curr_cell) in &curr_map {
            match prev_map.get(price) {
                Some(prev_cell) => {
                    if *prev_cell != *curr_cell {
                        changed.push((*price, (*curr_cell).clone()));
                    }
                }
                None => {
                    new_cells.push((*curr_cell).clone());
                }
            }
        }
        for price in prev_map.keys() {
            if !curr_map.contains_key(price) {
                removed.push(*price);
            }
        }

        let ps = &previous.summary;
        let cs = &current.summary;
        let summary_delta = HeatmapSummaryDelta {
            total_executed_buy: cs.total_executed_buy.saturating_sub(ps.total_executed_buy),
            total_executed_sell: cs
                .total_executed_sell
                .saturating_sub(ps.total_executed_sell),
            total_delta: cs.total_delta.wrapping_sub(ps.total_delta),
            total_trade_count: cs.total_trade_count.saturating_sub(ps.total_trade_count),
            total_liquidity_added: cs
                .total_liquidity_added
                .saturating_sub(ps.total_liquidity_added),
            total_liquidity_removed: cs
                .total_liquidity_removed
                .saturating_sub(ps.total_liquidity_removed),
            total_large_trade_volume: cs
                .total_large_trade_volume
                .saturating_sub(ps.total_large_trade_volume),
            total_replenishment_count: cs
                .total_replenishment_count
                .saturating_sub(ps.total_replenishment_count),
            total_absorption_candidate_count: cs
                .total_absorption_candidate_count
                .saturating_sub(ps.total_absorption_candidate_count),
            total_sweep_count: cs.total_sweep_count.saturating_sub(ps.total_sweep_count),
        };

        Self {
            changed,
            new: new_cells,
            removed,
            summary_delta,
        }
    }
}

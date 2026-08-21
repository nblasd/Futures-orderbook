//! Trade-flow analytics: trade delta, cumulative volume delta (CVD),
//! volume-at-price profile, and time-bucketed volume.
//!
//! ## Delta sign convention
//!
//! *positive delta = buyers aggressive*
//! *negative delta = sellers aggressive*
//!
//! `delta = buy_volume - sell_volume` where buy/sell refer to the aggressor
//! side of each trade.
//!
//! ## CVD
//!
//! `CVD(t) = previous_CVD + trade_delta`. Every aggressive BUY adds its
//! quantity; every aggressive SELL subtracts it. CVD is cumulative for the
//! session and is never auto-reset (windowed CVD is a derived view, optional).
//!
//! All quantities are integer ticks (1e8 scale) — no floating point.

use std::collections::{BTreeMap, VecDeque};

use crate::analytics::snapshot::AnalyticsFlowDigest;
use crate::trades::trade::{AggressorSide, TradeEvent};

/// Session-aggregated flow counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FlowTotals {
    pub trade_count: u64,
    pub buy_volume: u64,
    pub sell_volume: u64,
    pub cvd: i128,
}

impl FlowTotals {
    pub fn trade_volume(&self) -> u64 {
        self.buy_volume + self.sell_volume
    }
    pub fn delta(&self) -> i128 {
        self.buy_volume as i128 - self.sell_volume as i128
    }
}

/// Per-price flow profile (volume at price).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VolumeAtPrice {
    pub price: u64,
    pub buy_volume: u64,
    pub sell_volume: u64,
    pub total_volume: u64,
    pub delta: i128,
    pub trade_count: u64,
    pub large_trade_count: u64,
}

/// A finalized time bucket (100ms/1s/5s/1m by default).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeBucket {
    pub interval_ms: u64,
    pub start_ms: u64,
    pub end_ms: u64,
    pub buy_volume: u64,
    pub sell_volume: u64,
    pub total_volume: u64,
    pub delta: i128,
    pub trade_count: u64,
    /// Session CVD at the moment the bucket closed.
    pub cvd_end: i128,
}

impl TimeBucket {
    fn empty(interval_ms: u64, start_ms: u64) -> Self {
        Self {
            interval_ms,
            start_ms,
            end_ms: start_ms + interval_ms,
            buy_volume: 0,
            sell_volume: 0,
            total_volume: 0,
            delta: 0,
            trade_count: 0,
            cvd_end: 0,
        }
    }
}

/// Trade-flow engine state.
pub struct TradeFlow {
    totals: FlowTotals,
    volume_by_price: BTreeMap<u64, VolumeAtPrice>,
    /// Open bucket running totals per (interval_ms, start_ms).
    bucket_totals: BTreeMap<(u64, u64), TimeBucket>,
    /// Completed buckets, oldest first, bounded by retention.
    completed: VecDeque<TimeBucket>,
    /// The configured aggregation intervals.
    intervals: Vec<u64>,
    retention_ms: u64,
}

impl TradeFlow {
    pub fn new(intervals: Vec<u64>, retention_ms: u64) -> Self {
        Self {
            totals: FlowTotals::default(),
            volume_by_price: BTreeMap::new(),
            bucket_totals: BTreeMap::new(),
            completed: VecDeque::new(),
            intervals,
            retention_ms,
        }
    }

    pub fn totals(&self) -> &FlowTotals {
        &self.totals
    }

    /// Process an aggressive trade. Returns the finalized time buckets that
    /// were closed by this trade (one per interval when the bucket advanced).
    pub fn on_trade(&mut self, trade: &TradeEvent) -> Vec<TimeBucket> {
        let ts = trade.trade_time;
        let qty = trade.quantity_ticks;

        // Session totals.
        self.totals.trade_count += 1;
        match trade.aggressor {
            AggressorSide::Buy => {
                self.totals.buy_volume += qty;
                self.totals.cvd += qty as i128;
            }
            AggressorSide::Sell => {
                self.totals.sell_volume += qty;
                self.totals.cvd -= qty as i128;
            }
        }

        // Volume at price.
        let entry = self
            .volume_by_price
            .entry(trade.price_ticks)
            .or_insert_with(|| VolumeAtPrice {
                price: trade.price_ticks,
                ..Default::default()
            });
        entry.total_volume += qty;
        entry.trade_count += 1;
        match trade.aggressor {
            AggressorSide::Buy => {
                entry.buy_volume += qty;
                entry.delta += qty as i128;
            }
            AggressorSide::Sell => {
                entry.sell_volume += qty;
                entry.delta -= qty as i128;
            }
        }

        // Time buckets.
        let intervals = self.intervals.clone();
        let mut closed = Vec::new();
        for &interval in &intervals {
            let start = ts - (ts % interval);
            let key = (interval, start);
            match self.bucket_totals.get(&key) {
                Some(_) => {
                    // Bucket already open — accumulate below.
                }
                None => {
                    // A previous bucket for this interval (if any) is now
                    // complete; the newest open start is the previous start.
                    if let Some((&(_, prev_start), _)) = self
                        .bucket_totals
                        .range((interval, u64::MIN)..(interval, start))
                        .next_back()
                    {
                        closed.push(self.finalize_bucket(interval, prev_start, start));
                        self.bucket_totals.remove(&(interval, prev_start));
                    }
                    self.bucket_totals
                        .insert(key, TimeBucket::empty(interval, start));
                }
            }
            let bucket = self.bucket_totals.get_mut(&key).unwrap();
            bucket.total_volume += qty;
            bucket.trade_count += 1;
            match trade.aggressor {
                AggressorSide::Buy => {
                    bucket.buy_volume += qty;
                    bucket.delta += qty as i128;
                }
                AggressorSide::Sell => {
                    bucket.sell_volume += qty;
                    bucket.delta -= qty as i128;
                }
            }
        }
        closed
    }

    fn finalize_bucket(&mut self, interval: u64, start_ms: u64, end_ms: u64) -> TimeBucket {
        let mut bucket = self
            .bucket_totals
            .remove(&(interval, start_ms))
            .unwrap_or_else(|| TimeBucket::empty(interval, start_ms));
        bucket.end_ms = end_ms;
        bucket.cvd_end = self.totals.cvd;
        self.completed.push_back(bucket);
        bucket
    }

    /// Record that a large trade occurred at a price (updates the profile).
    pub fn record_large_trade(&mut self, price: u64) {
        if let Some(entry) = self.volume_by_price.get_mut(&price) {
            entry.large_trade_count += 1;
        }
    }

    /// Volume profile for a price range `[lo, hi]` (inclusive).
    pub fn volume_between(&self, lo: u64, hi: u64) -> impl Iterator<Item = &VolumeAtPrice> {
        self.volume_by_price.range(lo..=hi).map(|(_, v)| v)
    }

    pub fn volume_at(&self, price: u64) -> Option<&VolumeAtPrice> {
        self.volume_by_price.get(&price)
    }

    pub fn volume_by_price(&self) -> &BTreeMap<u64, VolumeAtPrice> {
        &self.volume_by_price
    }

    /// Completed time buckets for an interval, oldest first.
    pub fn completed_buckets(&self, interval_ms: u64) -> impl Iterator<Item = &TimeBucket> {
        self.completed
            .iter()
            .filter(move |b| b.interval_ms == interval_ms)
    }

    /// Drain completed buckets (e.g. for persistence). Deterministic order.
    pub fn take_completed(&mut self) -> Vec<TimeBucket> {
        let mut out: Vec<TimeBucket> = self.completed.drain(..).collect();
        out.sort_by_key(|b| (b.interval_ms, b.start_ms));
        out
    }

    /// Remove state older than the retention window.
    pub fn prune(&mut self, now_ms: u64) {
        let cutoff = now_ms.saturating_sub(self.retention_ms);
        while let Some(front) = self.completed.front() {
            if front.end_ms < cutoff {
                self.completed.pop_front();
            } else {
                break;
            }
        }
        // Volume-at-price is session-cumulative by design; it is bounded by the
        // number of distinct traded price levels, which is naturally limited.
    }

    /// Deterministic digest for live-vs-replay comparison.
    pub fn digest(&self) -> AnalyticsFlowDigest {
        let mut volume_by_price: Vec<(u64, u64, u64, u64, i128)> = self
            .volume_by_price
            .iter()
            .map(|(p, v)| (*p, v.total_volume, v.buy_volume, v.sell_volume, v.delta))
            .collect();
        volume_by_price.sort_unstable_by_key(|e| e.0);
        AnalyticsFlowDigest {
            trade_count: self.totals.trade_count,
            trade_volume: self.totals.trade_volume(),
            buy_volume: self.totals.buy_volume,
            sell_volume: self.totals.sell_volume,
            delta: self.totals.delta(),
            cvd: self.totals.cvd,
            volume_by_price,
            ..Default::default()
        }
    }
}

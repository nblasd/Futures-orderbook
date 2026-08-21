//! Liquidity tracking: additions/removals, per-level persistence, and
//! replenishment candidate detection.
//!
//! ## Important distinction
//!
//! A depth quantity decrease does **not** prove a trade consumed that
//! liquidity — the decrease could be a cancellation or a hidden-order
//! retraction. Analytics only observes displayed-quantity changes and never
//! claims causality.
//!
//! ## Replenishment
//!
//! When displayed quantity at a level decreases and then increases again at
//! the same price within `replenishment_window_ms`, we emit a
//! `LiquidityReplenishment` **candidate**. This is an observable pattern; it
//! does not prove an iceberg order (hidden order structure cannot be derived
//! from public market data).

use std::collections::{BTreeMap, VecDeque};

use crate::analytics::book::{BookSide, LevelChange};
use crate::analytics::config::AnalyticsConfig;
use crate::analytics::events::{AnalyticsEvent, AnalyticsEventKind};
use crate::orderbook::level::{PriceTick, QuantityTick};

/// Classified outcome of a level change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiquidityChangeKind {
    Added,
    Removed,
    Increased,
    Decreased,
    Unchanged,
}

impl LiquidityChangeKind {
    pub fn classify(old: Option<QuantityTick>, new: QuantityTick) -> Self {
        match (old, new) {
            (None, 0) => Self::Unchanged,
            (Some(o), n) if o == n => Self::Unchanged,
            (None, _) => Self::Added,
            (Some(_), 0) => Self::Removed,
            (Some(o), n) if n > o => Self::Increased,
            (Some(_), _) => Self::Decreased,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            LiquidityChangeKind::Added => "added",
            LiquidityChangeKind::Removed => "removed",
            LiquidityChangeKind::Increased => "increased",
            LiquidityChangeKind::Decreased => "decreased",
            LiquidityChangeKind::Unchanged => "unchanged",
        }
    }
}

/// Per-level persistence record (for the future heatmap and stacking/level
/// longevity analytics).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LevelPersistence {
    pub side: BookSide,
    pub price: PriceTick,
    pub created_at: u64,
    pub last_seen_at: u64,
    pub peak_quantity: QuantityTick,
    pub current_quantity: QuantityTick,
}

/// Liquidity analytics state.
pub struct LiquidityTracker {
    /// Session/interval cumulative liquidity added (ticks).
    pub added_ticks: u64,
    /// Session/interval cumulative liquidity removed (ticks).
    pub removed_ticks: u64,
    /// Per-level persistence records, keyed by (side, price).
    levels: BTreeMap<(BookSide, PriceTick), LevelPersistence>,
    /// Recent decreases per level, for replenishment detection.
    recent_decreases: BTreeMap<(BookSide, PriceTick), VecDeque<(u64, QuantityTick)>>,
    /// Number of replenishment candidates detected (session).
    pub replenishment_count: u64,
    cfg: AnalyticsConfig,
}

impl LiquidityTracker {
    pub fn new(cfg: &AnalyticsConfig) -> Self {
        Self {
            added_ticks: 0,
            removed_ticks: 0,
            levels: BTreeMap::new(),
            recent_decreases: BTreeMap::new(),
            replenishment_count: 0,
            cfg: cfg.clone(),
        }
    }

    /// Reset interval counters (called after each snapshot).
    pub fn reset_interval(&mut self) {
        self.added_ticks = 0;
        self.removed_ticks = 0;
    }

    /// Clear all level state (called on resync/snapshot rebuild).
    pub fn clear(&mut self) {
        self.levels.clear();
        self.recent_decreases.clear();
        self.added_ticks = 0;
        self.removed_ticks = 0;
    }

    /// Process a level change. Returns the analytics events it produced.
    pub fn on_level_change(&mut self, change: &LevelChange, ts: u64) -> Vec<AnalyticsEvent> {
        let mut out = Vec::new();
        let key = (change.side, change.price);
        let old = change.old_qty;
        let new = change.new_qty;

        match LiquidityChangeKind::classify(old, new) {
            LiquidityChangeKind::Unchanged => return out,
            LiquidityChangeKind::Added => {
                self.added_ticks += new;
                out.push(
                    AnalyticsEvent::new(AnalyticsEventKind::LiquidityAdded, ts, "BTCUSDT")
                        .with_side(change.side.as_str())
                        .with_price(change.price)
                        .with_quantity(new),
                );
            }
            LiquidityChangeKind::Removed => {
                let removed = old.unwrap_or(0);
                self.removed_ticks += removed;
                out.push(
                    AnalyticsEvent::new(AnalyticsEventKind::LiquidityRemoved, ts, "BTCUSDT")
                        .with_side(change.side.as_str())
                        .with_price(change.price)
                        .with_quantity(removed),
                );
            }
            LiquidityChangeKind::Increased => {
                let added = new.saturating_sub(old.unwrap_or(0));
                self.added_ticks += added;
                out.push(
                    AnalyticsEvent::new(AnalyticsEventKind::LiquidityIncreased, ts, "BTCUSDT")
                        .with_side(change.side.as_str())
                        .with_price(change.price)
                        .with_quantity(added),
                );
            }
            LiquidityChangeKind::Decreased => {
                let removed = old.unwrap_or(0).saturating_sub(new);
                self.removed_ticks += removed;
                // Record the decrease for replenishment detection.
                self.recent_decreases
                    .entry(key)
                    .or_default()
                    .push_back((ts, new));
                out.push(
                    AnalyticsEvent::new(AnalyticsEventKind::LiquidityDecreased, ts, "BTCUSDT")
                        .with_side(change.side.as_str())
                        .with_price(change.price)
                        .with_quantity(removed),
                );
            }
        }

        // Replenishment: an increase that follows a decrease at the same level
        // within the configured window.
        if matches!(new, _) && new > old.unwrap_or(0) {
            if let Some(decreases) = self.recent_decreases.get_mut(&key) {
                let window = self.cfg.replenishment_window_ms;
                let mut consumed: Option<usize> = None;
                for (i, (dts, _)) in decreases.iter().enumerate() {
                    if ts <= dts + window {
                        consumed = Some(i);
                        break;
                    }
                }
                if let Some(i) = consumed {
                    decreases.remove(i);
                    self.replenishment_count += 1;
                    out.push(
                        AnalyticsEvent::new(
                            AnalyticsEventKind::LiquidityReplenishment,
                            ts,
                            "BTCUSDT",
                        )
                        .with_side(change.side.as_str())
                        .with_price(change.price)
                        .with_quantity(new),
                    );
                }
            }
        }

        // Persistence record.
        match self.levels.get_mut(&key) {
            Some(rec) => {
                rec.last_seen_at = ts;
                rec.current_quantity = new;
                if new > rec.peak_quantity {
                    rec.peak_quantity = new;
                }
            }
            None => {
                self.levels.insert(
                    key,
                    LevelPersistence {
                        side: change.side,
                        price: change.price,
                        created_at: ts,
                        last_seen_at: ts,
                        peak_quantity: new,
                        current_quantity: new,
                    },
                );
            }
        }

        out
    }

    /// Remove per-level state that has not been seen for longer than the
    /// retention window (levels no longer displayed).
    pub fn prune(&mut self, now_ms: u64) {
        let cutoff = now_ms.saturating_sub(self.cfg.retention_ms);
        let stale: Vec<(BookSide, PriceTick)> = self
            .levels
            .iter()
            .filter(|(_, rec)| rec.last_seen_at < cutoff && rec.current_quantity == 0)
            .map(|(k, _)| *k)
            .collect();
        for k in stale {
            self.levels.remove(&k);
            self.recent_decreases.remove(&k);
        }
        // Trim decrease histories older than the replenishment window.
        for dq in self.recent_decreases.values_mut() {
            while let Some((dts, _)) = dq.front() {
                if *dts + self.cfg.replenishment_window_ms < now_ms {
                    dq.pop_front();
                } else {
                    break;
                }
            }
        }
    }

    pub fn level_count(&self) -> usize {
        self.levels.len()
    }

    pub fn persistence(&self) -> impl Iterator<Item = &LevelPersistence> {
        self.levels.values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> AnalyticsConfig {
        AnalyticsConfig::btcusdt_default()
    }

    fn change(side: BookSide, price: &str, old: Option<&str>, new: &str) -> LevelChange {
        let p = crate::orderbook::level::price_str_to_ticks(price).unwrap();
        let o = old.map(|s| crate::orderbook::level::quantity_str_to_ticks(s).unwrap());
        let n = crate::orderbook::level::quantity_str_to_ticks(new).unwrap();
        LevelChange {
            side,
            price: p,
            old_qty: o,
            new_qty: n,
        }
    }

    #[test]
    fn test_classify() {
        assert_eq!(
            LiquidityChangeKind::classify(None, 100),
            LiquidityChangeKind::Added
        );
        assert_eq!(
            LiquidityChangeKind::classify(Some(100), 0),
            LiquidityChangeKind::Removed
        );
        assert_eq!(
            LiquidityChangeKind::classify(Some(100), 200),
            LiquidityChangeKind::Increased
        );
        assert_eq!(
            LiquidityChangeKind::classify(Some(200), 100),
            LiquidityChangeKind::Decreased
        );
        assert_eq!(
            LiquidityChangeKind::classify(Some(100), 100),
            LiquidityChangeKind::Unchanged
        );
        assert_eq!(
            LiquidityChangeKind::classify(None, 0),
            LiquidityChangeKind::Unchanged
        );
    }

    #[test]
    fn test_added_removed_net() {
        let mut lt = LiquidityTracker::new(&cfg());
        let c = change(BookSide::Bid, "68000.00", None, "5.0");
        lt.on_level_change(&c, 1000);
        assert_eq!(lt.added_ticks, 500_000_000);
        assert_eq!(lt.removed_ticks, 0);

        let c = change(BookSide::Bid, "68000.00", Some("5.0"), "2.0");
        lt.on_level_change(&c, 1010);
        assert_eq!(lt.removed_ticks, 300_000_000);

        // 2 → 8 (increase) adds 6
        let c = change(BookSide::Bid, "68000.00", Some("2.0"), "8.0");
        lt.on_level_change(&c, 1020);
        assert_eq!(lt.added_ticks, 500_000_000 + 600_000_000);
    }

    #[test]
    fn test_replenishment_within_window() {
        let mut lt = LiquidityTracker::new(&cfg());
        // 10 → 2 (decrease)
        let c = change(BookSide::Ask, "68001.00", Some("10.0"), "2.0");
        let ev = lt.on_level_change(&c, 1000);
        assert!(!ev
            .iter()
            .any(|e| e.kind == AnalyticsEventKind::LiquidityReplenishment));
        // 2 → 9 (increase) within 250ms window
        let c = change(BookSide::Ask, "68001.00", Some("2.0"), "9.0");
        let ev = lt.on_level_change(&c, 1100);
        assert!(ev
            .iter()
            .any(|e| e.kind == AnalyticsEventKind::LiquidityReplenishment));
        assert_eq!(lt.replenishment_count, 1);
    }

    #[test]
    fn test_no_replenishment_outside_window() {
        let mut lt = LiquidityTracker::new(&cfg());
        let c = change(BookSide::Ask, "68001.00", Some("10.0"), "2.0");
        lt.on_level_change(&c, 1000);
        // Increase 1000ms later, far outside the 250ms window.
        let c = change(BookSide::Ask, "68001.00", Some("2.0"), "9.0");
        let ev = lt.on_level_change(&c, 2000);
        assert!(!ev
            .iter()
            .any(|e| e.kind == AnalyticsEventKind::LiquidityReplenishment));
        assert_eq!(lt.replenishment_count, 0);
    }

    #[test]
    fn test_persistence_record() {
        let mut lt = LiquidityTracker::new(&cfg());
        let c = change(BookSide::Bid, "68000.00", None, "3.0");
        lt.on_level_change(&c, 1000);
        let c = change(BookSide::Bid, "68000.00", Some("3.0"), "7.0");
        lt.on_level_change(&c, 1010);
        let rec = lt.persistence().next().unwrap();
        assert_eq!(rec.peak_quantity, 700_000_000);
        assert_eq!(rec.current_quantity, 700_000_000);
        assert_eq!(rec.created_at, 1000);
    }
}

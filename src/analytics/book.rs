//! Shadow order book maintained inside the analytics engine.
//!
//! The engine keeps its own copy of the book so it can compute liquidity
//! deltas (old vs new displayed quantity) exactly. It is fed from the *same*
//! `MarketEvent` stream as the live book, so live and replay produce
//! identical analytics. It never trusts the `best_bid`/`best_ask` fields on
//! `MarketEvent::OrderBookUpdated` — everything is derived from the changes.

use std::collections::BTreeMap;

use crate::orderbook::level::{PriceTick, QuantityTick, TICK_SCALE};

/// Order-book side discriminator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BookSide {
    Bid,
    Ask,
}

impl BookSide {
    pub fn as_str(self) -> &'static str {
        match self {
            BookSide::Bid => "BID",
            BookSide::Ask => "ASK",
        }
    }
}

/// A single price-level change derived from an order-book update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LevelChange {
    pub side: BookSide,
    pub price: PriceTick,
    /// Previous displayed quantity (None if the level did not exist).
    pub old_qty: Option<QuantityTick>,
    /// New displayed quantity (0 = level removed).
    pub new_qty: QuantityTick,
}

impl LevelChange {
    pub fn delta_ticks(&self) -> i128 {
        self.new_qty as i128 - self.old_qty.unwrap_or(0) as i128
    }
}

/// Exact rational representation of microprice: `num / den` in price ticks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Microprice {
    pub num: u128,
    pub den: u128,
}

impl Microprice {
    pub fn new(num: u128, den: u128) -> Option<Self> {
        if den == 0 {
            return None;
        }
        Some(Self { num, den })
    }

    pub fn f64(&self) -> f64 {
        self.num as f64 / self.den as f64 / TICK_SCALE as f64
    }
}

/// The analytics engine's authoritative view of the book.
#[derive(Debug, Clone)]
pub struct ShadowBook {
    /// Bids keyed by price (iterate ascending → best bid last).
    bids: BTreeMap<PriceTick, QuantityTick>,
    /// Asks keyed by price (iterate ascending → best ask first).
    asks: BTreeMap<PriceTick, QuantityTick>,
    ready: bool,
    last_update_id: Option<u64>,
    /// Number of times a crossed book was observed.
    crossed_count: u64,
    /// Number of invalid (negative / zero-price) levels rejected.
    invalid_levels_rejected: u64,
}

impl ShadowBook {
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            ready: false,
            last_update_id: None,
            crossed_count: 0,
            invalid_levels_rejected: 0,
        }
    }

    /// Replace all state from a full snapshot (initial sync or resync).
    /// Zero-quantity levels are dropped. Invalid levels are counted.
    pub fn apply_snapshot(
        &mut self,
        bids: &[(PriceTick, QuantityTick)],
        asks: &[(PriceTick, QuantityTick)],
        update_id: u64,
    ) {
        self.bids.clear();
        self.asks.clear();
        for &(price, qty) in bids {
            if qty > 0 {
                self.bids.insert(price, qty);
            } else {
                self.invalid_levels_rejected += 1;
            }
        }
        for &(price, qty) in asks {
            if qty > 0 {
                self.asks.insert(price, qty);
            } else {
                self.invalid_levels_rejected += 1;
            }
        }
        self.last_update_id = Some(update_id);
        self.ready = true;
    }

    /// Apply a diff-depth update, returning per-level changes.
    ///
    /// `old_qty` hints (when present) are ignored — the shadow book derives
    /// old quantities from its own state, which is what makes live and replay
    /// deterministic.
    pub fn apply_update(
        &mut self,
        bid_changes: &[(PriceTick, QuantityTick, Option<QuantityTick>)],
        ask_changes: &[(PriceTick, QuantityTick, Option<QuantityTick>)],
        update_id: u64,
    ) -> Vec<LevelChange> {
        let mut out = Vec::with_capacity(bid_changes.len() + ask_changes.len());
        for &(price, qty, _) in bid_changes {
            let old = self.bids.get(&price).copied();
            if qty == 0 {
                self.bids.remove(&price);
            } else {
                self.bids.insert(price, qty);
            }
            if old != Some(qty) {
                out.push(LevelChange {
                    side: BookSide::Bid,
                    price,
                    old_qty: old,
                    new_qty: qty,
                });
            }
        }
        for &(price, qty, _) in ask_changes {
            let old = self.asks.get(&price).copied();
            if qty == 0 {
                self.asks.remove(&price);
            } else {
                self.asks.insert(price, qty);
            }
            if old != Some(qty) {
                out.push(LevelChange {
                    side: BookSide::Ask,
                    price,
                    old_qty: old,
                    new_qty: qty,
                });
            }
        }
        self.last_update_id = Some(update_id);
        out
    }

    pub fn best_bid(&self) -> Option<PriceTick> {
        self.bids.keys().next_back().copied()
    }

    pub fn best_ask(&self) -> Option<PriceTick> {
        self.asks.keys().next().copied()
    }

    /// True when best bid >= best ask (anomalous state).
    pub fn is_crossed(&self) -> bool {
        match (self.best_bid(), self.best_ask()) {
            (Some(b), Some(a)) => b >= a,
            _ => false,
        }
    }

    pub fn spread_ticks(&self) -> Option<u64> {
        if self.is_crossed() {
            return None;
        }
        let bid = self.best_bid()?;
        let ask = self.best_ask()?;
        Some(ask - bid)
    }

    pub fn mid_price_f64(&self) -> Option<f64> {
        if self.is_crossed() {
            return None;
        }
        let bid = self.best_bid()?;
        let ask = self.best_ask()?;
        Some((bid as f64 + ask as f64) / 2.0 / TICK_SCALE as f64)
    }

    /// Sum of displayed liquidity in the top `depth` levels of each side.
    pub fn depth_volume(&self, depth: u32) -> (u64, u64) {
        let bid: u64 = self
            .bids
            .iter()
            .rev()
            .take(depth as usize)
            .map(|(_, q)| q)
            .sum();
        let ask: u64 = self.asks.iter().take(depth as usize).map(|(_, q)| q).sum();
        (bid, ask)
    }

    /// Order-book imbalance in [-1, +1]:
    /// `(bid_volume - ask_volume) / (bid_volume + ask_volume)`.
    ///
    /// Returns `None` when the book is not ready, crossed, or both depths are
    /// zero. +1 = entirely bid liquidity, -1 = entirely ask liquidity.
    pub fn imbalance(&self, depth: u32) -> Option<f64> {
        if !self.ready || self.is_crossed() {
            return None;
        }
        let (bid, ask) = self.depth_volume(depth);
        let total = bid + ask;
        if total == 0 {
            return None;
        }
        Some((bid as f64 - ask as f64) / total as f64)
    }

    /// Microprice = `(ask_price * bid_qty + bid_price * ask_qty) / (bid_qty + ask_qty)`
    /// using exact integer arithmetic (numerator/denominator in ticks).
    pub fn microprice(&self) -> Option<Microprice> {
        if !self.ready || self.is_crossed() {
            return None;
        }
        let bid = self.best_bid()?;
        let ask = self.best_ask()?;
        let bid_qty = *self.bids.get(&bid)?;
        let ask_qty = *self.asks.get(&ask)?;
        let den = bid_qty as u128 + ask_qty as u128;
        Microprice::new(
            (ask as u128) * (bid_qty as u128) + (bid as u128) * (ask_qty as u128),
            den,
        )
    }

    pub fn quantity_at(&self, side: BookSide, price: PriceTick) -> u64 {
        match side {
            BookSide::Bid => self.bids.get(&price).copied().unwrap_or(0),
            BookSide::Ask => self.asks.get(&price).copied().unwrap_or(0),
        }
    }

    pub fn is_ready(&self) -> bool {
        self.ready
    }

    pub fn set_ready(&mut self, ready: bool) {
        self.ready = ready;
        if !ready {
            self.bids.clear();
            self.asks.clear();
        }
    }

    pub fn last_update_id(&self) -> Option<u64> {
        self.last_update_id
    }

    pub fn crossed_count(&self) -> u64 {
        self.crossed_count
    }

    /// Record that a crossed-book state was observed. Returns the new total.
    pub fn record_crossed(&mut self) -> u64 {
        self.crossed_count += 1;
        self.crossed_count
    }

    pub fn invalid_levels_rejected(&self) -> u64 {
        self.invalid_levels_rejected
    }

    pub fn bid_levels(&self) -> impl Iterator<Item = (PriceTick, QuantityTick)> + '_ {
        self.bids.iter().rev().map(|(&p, &q)| (p, q))
    }

    pub fn ask_levels(&self) -> impl Iterator<Item = (PriceTick, QuantityTick)> + '_ {
        self.asks.iter().map(|(&p, &q)| (p, q))
    }

    pub fn bid_levels_top(&self, n: usize) -> Vec<(PriceTick, QuantityTick)> {
        self.bid_levels().take(n).collect()
    }

    pub fn ask_levels_top(&self, n: usize) -> Vec<(PriceTick, QuantityTick)> {
        self.ask_levels().take(n).collect()
    }
}

impl Default for ShadowBook {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(p: &str) -> u64 {
        crate::orderbook::level::price_str_to_ticks(p).unwrap()
    }
    fn q(p: &str) -> u64 {
        crate::orderbook::level::quantity_str_to_ticks(p).unwrap()
    }

    fn ready_book() -> ShadowBook {
        let mut b = ShadowBook::new();
        b.apply_snapshot(
            &[(t("68000.00"), q("10.0"))],
            &[(t("68001.00"), q("5.0"))],
            1,
        );
        b
    }

    #[test]
    fn test_snapshot_and_best() {
        let b = ready_book();
        assert!(b.is_ready());
        assert_eq!(b.best_bid(), Some(t("68000.00")));
        assert_eq!(b.best_ask(), Some(t("68001.00")));
        assert!(!b.is_crossed());
    }

    #[test]
    fn test_imbalance_exact() {
        let b = ready_book();
        // bid 10, ask 5 → (10-5)/(10+5) = 0.3333...
        let imb = b.imbalance(10).unwrap();
        assert!((imb - 5.0 / 15.0).abs() < 1e-9);
    }

    #[test]
    fn test_microprice_exact() {
        let b = ready_book();
        let mp = b.microprice().unwrap();
        // (68001.00*10 + 68000.00*5)/15 = (680010 + 340000)/15 = 1020010/15
        // in ticks: ask=6800100000000, bid=6800000000000, bid_qty=1000000000, ask_qty=500000000
        // num = 6800100000000*1000000000 + 6800000000000*500000000
        // den = 1000000000 + 500000000 = 1500000000
        let expected_num: u128 = (t("68001.00") as u128) * (q("10.0") as u128)
            + (t("68000.00") as u128) * (q("5.0") as u128);
        let expected_den: u128 = q("10.0") as u128 + q("5.0") as u128;
        assert_eq!(mp.num, expected_num);
        assert_eq!(mp.den, expected_den);
    }

    #[test]
    fn test_apply_update_tracks_old_qty() {
        let mut b = ready_book();
        let changes = b.apply_update(
            &[(t("68000.00"), q("8.0"), None)],
            &[(t("68001.00"), 0, None)],
            2,
        );
        assert_eq!(changes.len(), 2);
        let bid_change = changes.iter().find(|c| c.side == BookSide::Bid).unwrap();
        assert_eq!(bid_change.old_qty, Some(q("10.0")));
        assert_eq!(bid_change.new_qty, q("8.0"));
        assert_eq!(bid_change.delta_ticks(), -200_000_000);
    }

    #[test]
    fn test_crossed_book_protection() {
        let mut b = ShadowBook::new();
        b.apply_snapshot(
            &[(t("68001.00"), q("1.0"))],
            &[(t("68000.00"), q("1.0"))],
            1,
        );
        assert!(b.is_crossed());
        assert_eq!(b.imbalance(10), None);
        assert_eq!(b.microprice(), None);
        assert_eq!(b.spread_ticks(), None);
        assert_eq!(b.mid_price_f64(), None);
    }
}

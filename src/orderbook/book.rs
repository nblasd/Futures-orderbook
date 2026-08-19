use std::collections::BTreeMap;

use tracing::debug;

use super::level::{PriceLevel, PriceTick, QuantityTick, TICK_SCALE};

/// An immutable snapshot of the current order book state.
#[derive(Debug, Clone)]
pub struct OrderBookSnapshot {
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub last_update_id: u64,
    pub mid_price: Option<f64>,
    pub best_bid: Option<PriceTick>,
    pub best_ask: Option<PriceTick>,
}

/// The local order book.
///
/// Bids are stored in a BTreeMap keyed by negated price tick, so iterating
/// the map yields prices from highest (best bid) to lowest.
///
/// Asks are stored in a BTreeMap keyed by price tick, so iterating
/// the map yields prices from lowest (best ask) to highest.
///
/// This provides O(log n) insert/update/remove and O(1) best-price lookup.
#[derive(Debug)]
pub struct OrderBook {
    /// Bids keyed by NEGATED price tick (highest bid first when iterated).
    bids: BTreeMap<PriceTick, QuantityTick>,
    /// Asks keyed by price tick (lowest ask first when iterated).
    asks: BTreeMap<PriceTick, QuantityTick>,
    /// The last update ID from the most recently applied depth event.
    last_update_id: u64,
    /// Whether this book has been initialized from a snapshot.
    initialized: bool,
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_update_id: 0,
            initialized: false,
        }
    }

    /// Reset the book to empty state.
    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.last_update_id = 0;
        self.initialized = false;
    }

    /// Initialize the book from a REST snapshot.
    /// This replaces all existing state.
    pub fn apply_snapshot(
        &mut self,
        bids: &[(String, String)],
        asks: &[(String, String)],
        last_update_id: u64,
    ) -> anyhow::Result<()> {
        self.bids.clear();
        self.asks.clear();

        for (price_str, qty_str) in bids {
            let price = super::level::price_str_to_ticks(price_str)?;
            let qty = super::level::quantity_str_to_ticks(qty_str)?;
            if qty > 0 {
                // Negate for descending order in BTreeMap iteration
                self.bids.insert(price, qty);
            }
        }

        for (price_str, qty_str) in asks {
            let price = super::level::price_str_to_ticks(price_str)?;
            let qty = super::level::quantity_str_to_ticks(qty_str)?;
            if qty > 0 {
                self.asks.insert(price, qty);
            }
        }

        self.last_update_id = last_update_id;
        self.initialized = true;

        debug!(
            "Snapshot applied: {} bid levels, {} ask levels, last_update_id={}",
            self.bids.len(),
            self.asks.len(),
            last_update_id
        );

        Ok(())
    }

    /// Apply a single depth update from the WebSocket stream.
    ///
    /// Bid/ask updates are absolute quantities for price levels.
    /// If quantity is 0, the level is removed.
    /// If quantity is non-zero, the level is set to that quantity.
    pub fn apply_depth_update(
        &mut self,
        bids: &[(String, String)],
        asks: &[(String, String)],
        update_id: u64,
    ) -> anyhow::Result<()> {
        if !self.initialized {
            return Err(crate::error::OrderBookError::SnapshotRequired.into());
        }

        for (price_str, qty_str) in bids {
            let price = super::level::price_str_to_ticks(price_str)?;
            let qty = super::level::quantity_str_to_ticks(qty_str)?;
            if qty == 0 {
                self.bids.remove(&price);
                debug!("Removed bid level at price ticks={}", price);
            } else {
                self.bids.insert(price, qty);
            }
        }

        for (price_str, qty_str) in asks {
            let price = super::level::price_str_to_ticks(price_str)?;
            let qty = super::level::quantity_str_to_ticks(qty_str)?;
            if qty == 0 {
                self.asks.remove(&price);
                debug!("Removed ask level at price ticks={}", price);
            } else {
                self.asks.insert(price, qty);
            }
        }

        self.last_update_id = update_id;
        Ok(())
    }

    /// Get the best bid (highest bid price).
    pub fn best_bid(&self) -> Option<PriceTick> {
        // BTreeMap iterates in ascending key order.
        // Since bids are stored negated, the LAST element has the smallest negated key,
        // i.e., the highest actual price.
        self.bids.keys().next_back().copied()
    }

    /// Get the best ask (lowest ask price).
    pub fn best_ask(&self) -> Option<PriceTick> {
        self.asks.keys().next().copied()
    }

    /// Get the mid price as a floating-point value for display purposes.
    pub fn mid_price(&self) -> Option<f64> {
        let bid = self.best_bid()?;
        let ask = self.best_ask()?;
        Some(((bid + ask) as f64) / 2.0 / TICK_SCALE as f64)
    }

    /// Get the spread in price ticks.
    pub fn spread(&self) -> Option<PriceTick> {
        let bid = self.best_bid()?;
        let ask = self.best_ask()?;
        Some(ask - bid)
    }

    /// Get the number of bid levels.
    pub fn bid_count(&self) -> usize {
        self.bids.len()
    }

    /// Get the number of ask levels.
    pub fn ask_count(&self) -> usize {
        self.asks.len()
    }

    /// Get a snapshot of the current book state.
    pub fn snapshot(&self) -> OrderBookSnapshot {
        let bid_levels: Vec<PriceLevel> = self
            .bids
            .iter()
            .rev() // highest to lowest
            .map(|(&price, &qty)| PriceLevel::new(price, qty))
            .collect();

        let ask_levels: Vec<PriceLevel> = self
            .asks
            .iter() // lowest to highest
            .map(|(&price, &qty)| PriceLevel::new(price, qty))
            .collect();

        OrderBookSnapshot {
            bids: bid_levels,
            asks: ask_levels,
            last_update_id: self.last_update_id,
            mid_price: self.mid_price(),
            best_bid: self.best_bid(),
            best_ask: self.best_ask(),
        }
    }

    /// Get the top N bid levels.
    pub fn bid_levels(&self, n: usize) -> Vec<PriceLevel> {
        self.bids
            .iter()
            .rev()
            .take(n)
            .map(|(&price, &qty)| PriceLevel::new(price, qty))
            .collect()
    }

    /// Get the top N ask levels.
    pub fn ask_levels(&self, n: usize) -> Vec<PriceLevel> {
        self.asks
            .iter()
            .take(n)
            .map(|(&price, &qty)| PriceLevel::new(price, qty))
            .collect()
    }

    /// Get the last update ID.
    pub fn last_update_id(&self) -> u64 {
        self.last_update_id
    }

    /// Set the last update ID (used by the synchronizer).
    pub fn set_last_update_id(&mut self, id: u64) {
        self.last_update_id = id;
    }

    /// Check if the book is initialized (has received a snapshot).
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Set the initialized flag.
    pub fn set_initialized(&mut self, val: bool) {
        self.initialized = val;
    }

    /// Verify book invariants. Returns Ok(()) if valid.
    pub fn verify_invariants(&self) -> anyhow::Result<()> {
        // No zero-quantity levels
        for (&price, &qty) in &self.bids {
            if qty == 0 {
                anyhow::bail!("Zero-quantity bid level at price tick={}", price);
            }
        }
        for (&price, &qty) in &self.asks {
            if qty == 0 {
                anyhow::bail!("Zero-quantity ask level at price tick={}", price);
            }
        }

        // Best bid < best ask (when both sides are non-empty)
        if let (Some(bid), Some(ask)) = (self.best_bid(), self.best_ask()) {
            if bid >= ask {
                anyhow::bail!(
                    "Invariant violation: best_bid ({}) >= best_ask ({})",
                    bid,
                    ask
                );
            }
        }

        Ok(())
    }
}

impl Default for OrderBook {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orderbook::level::{price_str_to_ticks, quantity_str_to_ticks};

    #[test]
    fn test_empty_book() {
        let book = OrderBook::new();
        assert!(!book.is_initialized());
        assert_eq!(book.best_bid(), None);
        assert_eq!(book.best_ask(), None);
        assert_eq!(book.mid_price(), None);
        assert_eq!(book.spread(), None);
        assert_eq!(book.bid_count(), 0);
        assert_eq!(book.ask_count(), 0);
    }

    #[test]
    fn test_snapshot_creates_bids() {
        let mut book = OrderBook::new();
        let bids = vec![
            ("50000.10".to_string(), "1.5".to_string()),
            ("49999.90".to_string(), "2.0".to_string()),
        ];
        let asks = vec![];
        book.apply_snapshot(&bids, &asks, 100).unwrap();
        assert!(book.is_initialized());
        assert_eq!(book.bid_count(), 2);
        assert_eq!(
            book.best_bid(),
            Some(price_str_to_ticks("50000.10").unwrap())
        );
    }

    #[test]
    fn test_snapshot_creates_asks() {
        let mut book = OrderBook::new();
        let bids = vec![];
        let asks = vec![
            ("50000.20".to_string(), "0.5".to_string()),
            ("50000.30".to_string(), "3.0".to_string()),
        ];
        book.apply_snapshot(&bids, &asks, 100).unwrap();
        assert_eq!(book.ask_count(), 2);
        assert_eq!(
            book.best_ask(),
            Some(price_str_to_ticks("50000.20").unwrap())
        );
    }

    #[test]
    fn test_insert_new_bid() {
        let mut book = OrderBook::new();
        let bids = vec![("50000.10".to_string(), "1.0".to_string())];
        book.apply_snapshot(&bids, &[], 100).unwrap();

        // Add a new higher bid
        let new_bids = vec![("50001.00".to_string(), "0.5".to_string())];
        book.apply_depth_update(&new_bids, &[], 101).unwrap();
        assert_eq!(
            book.best_bid(),
            Some(price_str_to_ticks("50001.00").unwrap())
        );
        assert_eq!(book.bid_count(), 2);
    }

    #[test]
    fn test_insert_new_ask() {
        let mut book = OrderBook::new();
        let asks = vec![("50001.00".to_string(), "1.0".to_string())];
        book.apply_snapshot(&[], &asks, 100).unwrap();

        let new_asks = vec![("50000.50".to_string(), "0.5".to_string())];
        book.apply_depth_update(&[], &new_asks, 101).unwrap();
        assert_eq!(
            book.best_ask(),
            Some(price_str_to_ticks("50000.50").unwrap())
        );
        assert_eq!(book.ask_count(), 2);
    }

    #[test]
    fn test_update_existing_bid() {
        let mut book = OrderBook::new();
        let bids = vec![("50000.10".to_string(), "1.0".to_string())];
        book.apply_snapshot(&bids, &[], 100).unwrap();

        // Update the existing bid
        let update = vec![("50000.10".to_string(), "2.5".to_string())];
        book.apply_depth_update(&update, &[], 101).unwrap();
        assert_eq!(book.bid_count(), 1);
        // Check the level has the new quantity
        let levels = book.bid_levels(1);
        assert_eq!(levels[0].quantity, quantity_str_to_ticks("2.5").unwrap());
    }

    #[test]
    fn test_update_existing_ask() {
        let mut book = OrderBook::new();
        let asks = vec![("50001.00".to_string(), "1.0".to_string())];
        book.apply_snapshot(&[], &asks, 100).unwrap();

        let update = vec![("50001.00".to_string(), "3.0".to_string())];
        book.apply_depth_update(&[], &update, 101).unwrap();
        assert_eq!(book.ask_count(), 1);
        let levels = book.ask_levels(1);
        assert_eq!(levels[0].quantity, quantity_str_to_ticks("3.0").unwrap());
    }

    #[test]
    fn test_quantity_zero_removes_bid() {
        let mut book = OrderBook::new();
        let bids = vec![
            ("50000.10".to_string(), "1.0".to_string()),
            ("49999.90".to_string(), "2.0".to_string()),
        ];
        book.apply_snapshot(&bids, &[], 100).unwrap();
        assert_eq!(book.bid_count(), 2);

        let removal = vec![("50000.10".to_string(), "0".to_string())];
        book.apply_depth_update(&removal, &[], 101).unwrap();
        assert_eq!(book.bid_count(), 1);
        assert_eq!(
            book.best_bid(),
            Some(price_str_to_ticks("49999.90").unwrap())
        );
    }

    #[test]
    fn test_quantity_zero_removes_ask() {
        let mut book = OrderBook::new();
        let asks = vec![
            ("50000.20".to_string(), "0.5".to_string()),
            ("50000.30".to_string(), "3.0".to_string()),
        ];
        book.apply_snapshot(&[], &asks, 100).unwrap();
        assert_eq!(book.ask_count(), 2);

        let removal = vec![("50000.20".to_string(), "0".to_string())];
        book.apply_depth_update(&[], &removal, 101).unwrap();
        assert_eq!(book.ask_count(), 1);
        assert_eq!(
            book.best_ask(),
            Some(price_str_to_ticks("50000.30").unwrap())
        );
    }

    #[test]
    fn test_best_bid_is_correct() {
        let mut book = OrderBook::new();
        let bids = vec![
            ("49998.00".to_string(), "1.0".to_string()),
            ("50000.10".to_string(), "2.0".to_string()),
            ("49999.50".to_string(), "3.0".to_string()),
        ];
        book.apply_snapshot(&bids, &[], 100).unwrap();
        assert_eq!(
            book.best_bid(),
            Some(price_str_to_ticks("50000.10").unwrap())
        );
    }

    #[test]
    fn test_best_ask_is_correct() {
        let mut book = OrderBook::new();
        let asks = vec![
            ("50001.50".to_string(), "1.0".to_string()),
            ("50000.20".to_string(), "2.0".to_string()),
            ("50002.00".to_string(), "3.0".to_string()),
        ];
        book.apply_snapshot(&[], &asks, 100).unwrap();
        assert_eq!(
            book.best_ask(),
            Some(price_str_to_ticks("50000.20").unwrap())
        );
    }

    #[test]
    fn test_mid_price() {
        let mut book = OrderBook::new();
        let bids = vec![("50000.00".to_string(), "1.0".to_string())];
        let asks = vec![("50001.00".to_string(), "1.0".to_string())];
        book.apply_snapshot(&bids, &asks, 100).unwrap();
        let mid = book.mid_price().unwrap();
        assert!((mid - 50000.50).abs() < 0.01);
    }

    #[test]
    fn test_spread() {
        let mut book = OrderBook::new();
        let bids = vec![("50000.00".to_string(), "1.0".to_string())];
        let asks = vec![("50000.50".to_string(), "1.0".to_string())];
        book.apply_snapshot(&bids, &asks, 100).unwrap();
        let spread = book.spread().unwrap();
        let expected_spread =
            price_str_to_ticks("50000.50").unwrap() - price_str_to_ticks("50000.00").unwrap();
        assert_eq!(spread, expected_spread);
    }
}

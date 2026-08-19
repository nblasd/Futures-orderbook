use std::collections::VecDeque;

use tracing::debug;

use super::trade::TradeEvent;

/// Maximum number of recent trade IDs to retain for duplicate detection.
/// This creates a bounded memory footprint while catching duplicates
/// within a reasonable window (typically hundreds of recent trades).
const DUPLICATE_WINDOW: usize = 4096;

/// Result of processing a single trade event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeProcessResult {
    /// Trade was processed normally.
    Processed,
    /// Trade was a duplicate (same trade ID seen before).
    Duplicate,
    /// Trade was stale (trade ID older than the last processed ID).
    Stale,
    /// Trade was ignored for other reasons.
    Ignored,
}

/// Trade ingestion processor.
///
/// Handles duplicate detection, ordering validation, and metrics tracking.
/// The processor does NOT depend on the order book — trade ingestion and
/// depth ingestion are intentionally independent.
pub struct TradeProcessor {
    /// The highest trade ID processed so far.
    last_trade_id: Option<u64>,
    /// Bounded set of recently-seen trade IDs for duplicate detection.
    recent_ids: VecDeque<u64>,
    /// Total trade events received from WebSocket.
    trade_events_received: u64,
    /// Total trade events successfully processed.
    trade_events_processed: u64,
    /// Total duplicate trades detected.
    duplicate_trades: u64,
    /// Total stale trades (older than last processed).
    stale_trades: u64,
    /// Total malformed trade events.
    malformed_trade_events: u64,
    /// Trade stream reconnect count.
    trade_reconnect_count: u64,
    /// Buy aggressor count.
    buy_aggressor_count: u64,
    /// Sell aggressor count.
    sell_aggressor_count: u64,
    /// Last processed trade (for diagnostics display).
    last_trade: Option<TradeEvent>,
}

impl TradeProcessor {
    pub fn new() -> Self {
        Self {
            last_trade_id: None,
            recent_ids: VecDeque::with_capacity(DUPLICATE_WINDOW),
            trade_events_received: 0,
            trade_events_processed: 0,
            duplicate_trades: 0,
            stale_trades: 0,
            malformed_trade_events: 0,
            trade_reconnect_count: 0,
            buy_aggressor_count: 0,
            sell_aggressor_count: 0,
            last_trade: None,
        }
    }

    /// Process a normalized trade event.
    pub fn process(&mut self, event: TradeEvent) -> TradeProcessResult {
        self.trade_events_received += 1;

        // Check for duplicate trade ID
        if self.recent_ids.contains(&event.trade_id) {
            self.duplicate_trades += 1;
            debug!("Duplicate trade ID: {}", event.trade_id);
            return TradeProcessResult::Duplicate;
        }

        // Check ordering: trade ID should be greater than or equal to last
        // We allow equality for the first trade, and >= for subsequent trades
        // because Binance may not guarantee strictly monotonic trade IDs across
        // all scenarios. However, if trade ID is strictly less than last, it's stale.
        if let Some(last_id) = self.last_trade_id {
            if event.trade_id < last_id {
                self.stale_trades += 1;
                debug!("Stale trade: id={} < last_id={}", event.trade_id, last_id);
                return TradeProcessResult::Stale;
            }
        }

        // Update tracking state
        self.last_trade_id = Some(event.trade_id);

        // Maintain bounded duplicate window
        self.recent_ids.push_back(event.trade_id);
        if self.recent_ids.len() > DUPLICATE_WINDOW {
            self.recent_ids.pop_front();
        }

        // Track aggressor side counts
        match event.aggressor {
            super::trade::AggressorSide::Buy => self.buy_aggressor_count += 1,
            super::trade::AggressorSide::Sell => self.sell_aggressor_count += 1,
        }

        self.trade_events_processed += 1;
        self.last_trade = Some(event);

        TradeProcessResult::Processed
    }

    /// Record that the trade stream reconnected.
    pub fn on_trade_reconnect(&mut self) {
        self.trade_reconnect_count += 1;
    }

    /// Record a malformed trade event.
    pub fn record_malformed(&mut self) {
        self.malformed_trade_events += 1;
    }

    // --- Metrics accessors ---

    pub fn trade_events_received(&self) -> u64 {
        self.trade_events_received
    }

    pub fn trade_events_processed(&self) -> u64 {
        self.trade_events_processed
    }

    pub fn duplicate_trades(&self) -> u64 {
        self.duplicate_trades
    }

    pub fn stale_trades(&self) -> u64 {
        self.stale_trades
    }

    pub fn malformed_trade_events(&self) -> u64 {
        self.malformed_trade_events
    }

    pub fn trade_reconnect_count(&self) -> u64 {
        self.trade_reconnect_count
    }

    pub fn last_trade_id(&self) -> Option<u64> {
        self.last_trade_id
    }

    pub fn last_trade(&self) -> Option<&TradeEvent> {
        self.last_trade.as_ref()
    }

    pub fn buy_aggressor_count(&self) -> u64 {
        self.buy_aggressor_count
    }

    pub fn sell_aggressor_count(&self) -> u64 {
        self.sell_aggressor_count
    }
}

impl Default for TradeProcessor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binance::trade_types::FuturesTrade;
    use crate::trades::normalizer::normalize_trade;

    fn make_raw_trade(trade_id: u64, price: &str, qty: &str, is_buyer_maker: bool) -> FuturesTrade {
        FuturesTrade {
            event_type: "trade".to_string(),
            event_time: 1787137583835,
            trade_time: 1787137583835,
            symbol: "BTCUSDT".to_string(),
            trade_id,
            price: price.to_string(),
            quantity: qty.to_string(),
            order_type: "MARKET".to_string(),
            is_buyer_maker,
            trade_type: 1,
        }
    }

    #[test]
    fn test_process_normal_trade() {
        let mut proc = TradeProcessor::new();
        let raw = make_raw_trade(100, "64000.00", "0.01", false);
        let event = normalize_trade(&raw).unwrap();
        let result = proc.process(event);
        assert_eq!(result, TradeProcessResult::Processed);
        assert_eq!(proc.trade_events_received(), 1);
        assert_eq!(proc.trade_events_processed(), 1);
        assert_eq!(proc.last_trade_id(), Some(100));
    }

    #[test]
    fn test_duplicate_detection() {
        let mut proc = TradeProcessor::new();
        let raw = make_raw_trade(100, "64000.00", "0.01", false);
        let event1 = normalize_trade(&raw).unwrap();
        let event2 = normalize_trade(&raw).unwrap();

        assert_eq!(proc.process(event1), TradeProcessResult::Processed);
        assert_eq!(proc.process(event2), TradeProcessResult::Duplicate);
        assert_eq!(proc.duplicate_trades(), 1);
        assert_eq!(proc.trade_events_processed(), 1); // Only 1 processed
    }

    #[test]
    fn test_stale_trade_detection() {
        let mut proc = TradeProcessor::new();
        let raw_old = make_raw_trade(50, "64000.00", "0.01", false);
        let raw_new = make_raw_trade(100, "64000.00", "0.01", false);
        let raw_older = make_raw_trade(40, "64000.00", "0.01", false);

        let event_old = normalize_trade(&raw_old).unwrap();
        let event_new = normalize_trade(&raw_new).unwrap();
        let event_older = normalize_trade(&raw_older).unwrap();

        assert_eq!(proc.process(event_old), TradeProcessResult::Processed);
        assert_eq!(proc.process(event_new), TradeProcessResult::Processed);
        assert_eq!(proc.process(event_older), TradeProcessResult::Stale);
        assert_eq!(proc.stale_trades(), 1);
    }

    #[test]
    fn test_buyer_maker_true_sell_aggressor() {
        let mut proc = TradeProcessor::new();
        let raw = make_raw_trade(1, "64000.00", "0.01", true);
        let event = normalize_trade(&raw).unwrap();
        proc.process(event);
        assert_eq!(proc.sell_aggressor_count(), 1);
        assert_eq!(proc.buy_aggressor_count(), 0);
    }

    #[test]
    fn test_buyer_maker_false_buy_aggressor() {
        let mut proc = TradeProcessor::new();
        let raw = make_raw_trade(1, "64000.00", "0.01", false);
        let event = normalize_trade(&raw).unwrap();
        proc.process(event);
        assert_eq!(proc.buy_aggressor_count(), 1);
        assert_eq!(proc.sell_aggressor_count(), 0);
    }

    #[test]
    fn test_reconnect_counter() {
        let mut proc = TradeProcessor::new();
        assert_eq!(proc.trade_reconnect_count(), 0);
        proc.on_trade_reconnect();
        assert_eq!(proc.trade_reconnect_count(), 1);
        proc.on_trade_reconnect();
        assert_eq!(proc.trade_reconnect_count(), 2);
    }

    #[test]
    fn test_malformed_counter() {
        let mut proc = TradeProcessor::new();
        proc.record_malformed();
        assert_eq!(proc.malformed_trade_events(), 1);
    }

    #[test]
    fn test_last_trade_stored() {
        let mut proc = TradeProcessor::new();
        assert!(proc.last_trade().is_none());

        let raw = make_raw_trade(1, "64000.00", "0.01", false);
        let event = normalize_trade(&raw).unwrap();
        proc.process(event);

        let last = proc.last_trade().unwrap();
        assert_eq!(last.trade_id, 1);
        assert_eq!(last.aggressor, crate::trades::trade::AggressorSide::Buy);
    }

    #[test]
    fn test_sequential_trades() {
        let mut proc = TradeProcessor::new();
        for i in 1..=100 {
            let raw = make_raw_trade(i, "64000.00", "0.01", i % 2 == 0);
            let event = normalize_trade(&raw).unwrap();
            assert_eq!(proc.process(event), TradeProcessResult::Processed);
        }
        assert_eq!(proc.trade_events_processed(), 100);
        assert_eq!(proc.last_trade_id(), Some(100));
        // 50 buys (odd IDs) + 50 sells (even IDs)
        assert_eq!(proc.buy_aggressor_count(), 50);
        assert_eq!(proc.sell_aggressor_count(), 50);
    }

    #[test]
    fn test_duplicate_window_eviction() {
        let mut proc = TradeProcessor::new();
        // Fill the window beyond DUPLICATE_WINDOW
        let base_id = 100_000;
        for i in 0..(DUPLICATE_WINDOW as u64 + 100) {
            let raw = make_raw_trade(base_id + i, "64000.00", "0.01", false);
            let event = normalize_trade(&raw).unwrap();
            proc.process(event);
        }
        // The very first ID (base_id) should have been evicted from the window
        // and should NOT be detected as duplicate if we see it again
        let raw = make_raw_trade(base_id, "64000.00", "0.01", false);
        let event = normalize_trade(&raw).unwrap();
        // This will be Stale (trade_id < last_trade_id) not Duplicate
        assert_eq!(proc.process(event), TradeProcessResult::Stale);
    }
}

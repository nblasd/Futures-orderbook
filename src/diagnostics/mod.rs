use std::time::Instant;

use crate::orderbook::{OrderBook, SyncState, Synchronizer};
use crate::trades::processor::TradeProcessor;

/// Metrics tracked by the order-book engine.
#[derive(Debug, Default)]
pub struct Metrics {
    pub events_received: u64,
    pub events_applied: u64,
    pub events_ignored: u64,
    pub malformed_events: u64,
    pub sequence_errors: u64,
    pub resync_count: u64,
    pub reconnect_count: u64,
    pub current_update_id: u64,
    pub last_event_time: Option<Instant>,
    pub connection_start: Option<Instant>,
}

impl Metrics {
    /// Create metrics from synchronizer state.
    pub fn from_synchronizer(sync: &Synchronizer, book: &OrderBook) -> Self {
        Self {
            events_received: sync.events_received(),
            events_applied: sync.events_applied(),
            events_ignored: sync.events_ignored(),
            malformed_events: 0,
            sequence_errors: sync.sequence_errors(),
            resync_count: sync.resync_count(),
            reconnect_count: sync.reconnect_count(),
            current_update_id: sync.last_applied_u().unwrap_or(book.last_update_id()),
            last_event_time: sync.last_applied_u().map(|_| Instant::now()),
            connection_start: None,
        }
    }

    /// Record that an event was received.
    pub fn record_event_received(&mut self) {
        self.events_received += 1;
    }

    /// Record that an event was applied.
    pub fn record_event_applied(&mut self) {
        self.events_applied += 1;
        self.last_event_time = Some(Instant::now());
    }

    /// Record that an event was ignored.
    pub fn record_event_ignored(&mut self) {
        self.events_ignored += 1;
    }

    /// Record a malformed event.
    pub fn record_malformed_event(&mut self) {
        self.malformed_events += 1;
    }

    /// Record a sequence error.
    pub fn record_sequence_error(&mut self) {
        self.sequence_errors += 1;
    }
}

/// Format the combined diagnostic display for the CLI (order book + trades).
pub fn format_diagnostics(
    symbol: &str,
    state: SyncState,
    book: &OrderBook,
    sync: &Synchronizer,
    trade_proc: &TradeProcessor,
    trade_connected: bool,
    start_time: Instant,
) -> String {
    let mut output = String::new();

    output.push_str(&format!("{} PERPETUAL\n", symbol));
    output.push_str("Market: Binance USDⓈ-M Futures\n\n");

    // --- Order Book ---
    output.push_str("Order Book\n");
    output.push_str(&format!("Status: {:?}\n", state));

    if let Some(bid) = book.best_bid() {
        output.push_str(&format!(
            "Best Bid:  {}\n",
            crate::orderbook::level::ticks_to_price_str(bid)
        ));
    } else {
        output.push_str("Best Bid:  -----\n");
    }

    if let Some(ask) = book.best_ask() {
        output.push_str(&format!(
            "Best Ask:  {}\n",
            crate::orderbook::level::ticks_to_price_str(ask)
        ));
    } else {
        output.push_str("Best Ask:  -----\n");
    }

    if let Some(mid) = book.mid_price() {
        output.push_str(&format!("Mid:       {:.2}\n", mid));
    } else {
        output.push_str("Mid:       -----\n");
    }

    if let Some(spread) = book.spread() {
        output.push_str(&format!(
            "Spread:    {}\n",
            crate::orderbook::level::ticks_to_price_str(spread)
        ));
    } else {
        output.push_str("Spread:    -----\n");
    }

    output.push('\n');

    output.push_str(&format!(
        "Last Update ID: {}\n",
        sync.last_applied_u().unwrap_or(book.last_update_id())
    ));
    output.push_str(&format!("Events Received: {}\n", sync.events_received()));
    output.push_str(&format!("Events Applied:  {}\n", sync.events_applied()));
    output.push_str(&format!("Events Ignored:  {}\n", sync.events_ignored()));
    output.push_str(&format!("Resyncs: {}\n", sync.resync_count()));
    output.push_str(&format!("Reconnects: {}\n", sync.reconnect_count()));
    output.push_str(&format!("Sequence Errors: {}\n", sync.sequence_errors()));
    output.push('\n');

    // --- Trades ---
    output.push_str("Trades\n");
    output.push_str(&format!(
        "Status: {}\n",
        if trade_connected {
            "CONNECTED"
        } else {
            "DISCONNECTED"
        }
    ));
    output.push_str(&format!(
        "Trades Received: {}\n",
        trade_proc.trade_events_received()
    ));
    output.push_str(&format!(
        "Trades Processed: {}\n",
        trade_proc.trade_events_processed()
    ));
    output.push_str(&format!("Duplicates: {}\n", trade_proc.duplicate_trades()));
    output.push_str(&format!("Stale: {}\n", trade_proc.stale_trades()));
    output.push_str(&format!(
        "Marker Events Rejected: {}\n",
        trade_proc.marker_events_rejected()
    ));
    output.push_str(&format!(
        "Buy Aggressors: {}\n",
        trade_proc.buy_aggressor_count()
    ));
    output.push_str(&format!(
        "Sell Aggressors: {}\n",
        trade_proc.sell_aggressor_count()
    ));

    if let Some(last_trade) = trade_proc.last_trade() {
        output.push('\n');
        output.push_str("Last Trade:\n");
        output.push_str(&format!(
            "  Price:     {}\n",
            crate::orderbook::level::ticks_to_price_str(last_trade.price_ticks)
        ));
        output.push_str(&format!(
            "  Quantity:  {}\n",
            crate::orderbook::level::ticks_to_quantity_str(last_trade.quantity_ticks)
        ));
        output.push_str(&format!("  Aggressor: {}\n", last_trade.aggressor.label()));
        output.push_str(&format!("  Trade ID:  {}\n", last_trade.trade_id));
    }

    output.push('\n');

    let elapsed = start_time.elapsed().as_secs();
    output.push_str(&format!("Uptime: {}s\n", elapsed));

    output.push_str(&format!("Buffer size: {}\n", sync.buffer_size()));

    output
}

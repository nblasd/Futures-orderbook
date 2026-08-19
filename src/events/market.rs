/// Internal market events for future phases.
///
/// These events will drive the heatmap, CVD, absorption, sweeps,
/// and other indicators in later phases. For now, only order-book
/// related events are emitted.
#[derive(Debug, Clone)]
pub enum MarketEvent {
    /// The order book has been synchronized and is live.
    OrderBookSynchronized {
        symbol: String,
        last_update_id: u64,
        bid_levels: usize,
        ask_levels: usize,
    },

    /// The order book has been updated with new depth data.
    /// Contains enough information for future phases to compute:
    /// - Liquidity changes at each level
    /// - Historical liquidity snapshots
    /// - Volume/order-flow relationships
    /// - Price movement
    /// - Absorption patterns
    /// - Sweep detection
    OrderBookUpdated {
        symbol: String,
        update_id: u64,
        /// Bid changes: (price_ticks, new_quantity_ticks, old_quantity_ticks)
        bid_changes: Vec<(u64, u64, Option<u64>)>,
        /// Ask changes: (price_ticks, new_quantity_ticks, old_quantity_ticks)
        ask_changes: Vec<(u64, u64, Option<u64>)>,
        best_bid: Option<u64>,
        best_ask: Option<u64>,
        mid_price: Option<f64>,
    },

    /// Resynchronization has started.
    OrderBookResyncStarted { symbol: String, reason: String },

    /// Resynchronization has completed.
    OrderBookResyncCompleted { symbol: String, last_update_id: u64 },

    /// Connection status has changed.
    ConnectionStatusChanged {
        symbol: String,
        connected: bool,
        reconnect_count: u64,
    },
}

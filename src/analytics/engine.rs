//! The Phase 4 analytics engine — a deterministic, rule-based microstructure
//! analytics pipeline.
//!
//! The engine consumes the **same** [`MarketEvent`] stream in live mode and in
//! replay, so both produce identical results. It keeps its own shadow book so
//! liquidity deltas are exact (the `old_qty` on `OrderBookUpdated` is always
//! `None` from the wire) and never trusts `best_bid`/`best_ask`/`mid_price`
//! hints — those are derived from the changes.
//!
//! ## Determinism contract
//!
//! * Only exchange event timestamps are used (never local receive time).
//! * All iteration is over `BTreeMap`/`VecDeque` (deterministic order).
//! * All prices/quantities are integer ticks; no floating point for
//!   authoritative values.

use crate::analytics::absorption::AbsorptionDetector;
use crate::analytics::book::ShadowBook;
use crate::analytics::clusters::ClusterTracker;
use crate::analytics::config::AnalyticsConfig;
use crate::analytics::events::{AnalyticsEvent, AnalyticsEventKind};
use crate::analytics::flow::TradeFlow;
use crate::analytics::heatmap::Heatmap;
use crate::analytics::large_trades::LargeTradeDetector;
use crate::analytics::liquidity::LiquidityTracker;
use crate::analytics::snapshot::{AnalyticsFlowDigest, MarketMicrostructureSnapshot};
use crate::analytics::sweeps::SweepDetector;
use crate::events::market::MarketEvent;
use crate::trades::trade::{AggressorSide, TradeEvent};

/// Output of a single `process_event` call.
#[derive(Debug, Default)]
pub struct EngineOutput {
    /// Derived analytics events.
    pub events: Vec<AnalyticsEvent>,
    /// Snapshot(s) finalized during this event's processing.
    pub snapshots: Vec<MarketMicrostructureSnapshot>,
}

/// The analytics engine.
pub struct AnalyticsEngine {
    pub cfg: AnalyticsConfig,
    pub book: ShadowBook,
    flow: TradeFlow,
    liquidity: LiquidityTracker,
    large_trades: LargeTradeDetector,
    clusters: ClusterTracker,
    sweeps: SweepDetector,
    absorption: AbsorptionDetector,
    heatmap: Heatmap,

    // --- Interval counters (reset after each snapshot) ---
    trade_volume: u64,
    buy_volume: u64,
    sell_volume: u64,
    delta: i128,
    large_trade_count: u64,
    sweep_candidate_count: u64,
    absorption_candidate_count: u64,
    replenishment_count: u64,
    anomalies: u64,

    last_event_ts: Option<u64>,
    last_snapshot_ts: Option<u64>,
    /// Book was observed crossed at least once since the last snapshot.
    book_crossed: bool,
}

impl AnalyticsEngine {
    pub fn new(cfg: AnalyticsConfig) -> Self {
        Self {
            book: ShadowBook::new(),
            flow: TradeFlow::new(cfg.aggregation_intervals_ms.clone(), cfg.retention_ms),
            liquidity: LiquidityTracker::new(&cfg),
            large_trades: LargeTradeDetector::new(&cfg),
            clusters: ClusterTracker::new(&cfg),
            sweeps: SweepDetector::new(&cfg),
            absorption: AbsorptionDetector::new(&cfg),
            heatmap: Heatmap::new(&cfg),
            trade_volume: 0,
            buy_volume: 0,
            sell_volume: 0,
            delta: 0,
            large_trade_count: 0,
            sweep_candidate_count: 0,
            absorption_candidate_count: 0,
            replenishment_count: 0,
            anomalies: 0,
            last_event_ts: None,
            last_snapshot_ts: None,
            book_crossed: false,
            cfg,
        }
    }

    /// Process a raw market event. Produces derived analytics events and
    /// (possibly) finalized snapshots.
    pub fn process_event(&mut self, ev: &MarketEvent) -> EngineOutput {
        let mut out = EngineOutput::default();
        match ev {
            MarketEvent::OrderBookSynchronized { symbol, .. } => {
                // The shadow book becomes populated by the OrderBookSnapshot
                // that follows; nothing to do here but mark readiness.
                self.book.set_ready(true);
                let _ = symbol;
            }
            MarketEvent::OrderBookSnapshot {
                symbol,
                update_id,
                bids,
                asks,
            } => {
                self.book.apply_snapshot(bids, asks, *update_id);
                self.liquidity.clear();
                self.book_crossed = self.book.is_crossed();
                if self.book_crossed {
                    self.book.record_crossed();
                    self.anomalies += 1;
                    out.events.push(self.anomaly_event(
                        *update_id,
                        symbol,
                        "crossed book after snapshot",
                    ));
                }
            }
            MarketEvent::OrderBookUpdated {
                symbol,
                update_id,
                event_time_ms,
                bid_changes,
                ask_changes,
                ..
            } => {
                self.last_event_ts = Some(*event_time_ms);
                let changes = self.book.apply_update(bid_changes, ask_changes, *update_id);
                for change in &changes {
                    let mut evs = self.liquidity.on_level_change(change, *event_time_ms);
                    // Feed level change into the heatmap for resting liquidity.
                    self.heatmap.on_level_change(change, *event_time_ms);
                    // Feed replenishment evidence into absorption windows.
                    if change.new_qty > change.old_qty.unwrap_or(0) {
                        self.absorption.on_liquidity_change(
                            change.price,
                            change.new_qty,
                            *event_time_ms,
                        );
                    }
                    self.absorption.on_opposing_liquidity(
                        change.price,
                        change.new_qty,
                        *event_time_ms,
                    );
                    // Map replenishment events into heatmap cells.
                    for ev in &evs {
                        if ev.kind == AnalyticsEventKind::LiquidityReplenishment {
                            if let Some(price) = ev.price {
                                self.heatmap.on_replenishment(price, ev.ts_ms);
                                self.replenishment_count += 1;
                            }
                        }
                    }
                    out.events.append(&mut evs);
                }
                // Book-state anomalies.
                if self.book.is_crossed() {
                    if !self.book_crossed {
                        self.book.record_crossed();
                        self.anomalies += 1;
                        out.events.push(self.anomaly_event(
                            *update_id,
                            symbol,
                            "crossed book after update",
                        ));
                    }
                    self.book_crossed = true;
                } else {
                    self.book_crossed = false;
                }
                self.maybe_finalize_snapshot(&mut out, *event_time_ms);
            }
            MarketEvent::OrderBookResyncStarted { symbol, reason } => {
                self.book.set_ready(false);
                self.liquidity.clear();
                let _ = symbol;
                let _ = reason;
            }
            MarketEvent::OrderBookResyncCompleted { symbol, .. } => {
                // The OrderBookSnapshot that follows repopulates the book.
                let _ = symbol;
            }
            MarketEvent::Trade(trade) => {
                self.on_trade(trade, &mut out);
            }
            MarketEvent::ConnectionStatusChanged {
                symbol, connected, ..
            } => {
                if !connected {
                    self.anomalies += 1;
                    out.events.push(
                        AnalyticsEvent::new(
                            AnalyticsEventKind::BookAnomaly,
                            self.last_event_ts.unwrap_or(0),
                            symbol,
                        )
                        .with_detail(serde_json::json!({ "reason": "connection lost" })),
                    );
                }
            }
        }
        self.prune(self.ts_of_ev(ev));
        out
    }

    fn on_trade(&mut self, trade: &TradeEvent, out: &mut EngineOutput) {
        let ts = trade.trade_time;
        self.last_event_ts = Some(ts);
        self.heatmap.on_trade(trade);

        // Trade flow + interval counters.
        self.flow.on_trade(trade);
        self.trade_volume += trade.quantity_ticks;
        self.delta += match trade.aggressor {
            AggressorSide::Buy => trade.quantity_ticks as i128,
            AggressorSide::Sell => -(trade.quantity_ticks as i128),
        };
        match trade.aggressor {
            AggressorSide::Buy => self.buy_volume += trade.quantity_ticks,
            AggressorSide::Sell => self.sell_volume += trade.quantity_ticks,
        }

        // TradeDelta event.
        out.events.push(
            AnalyticsEvent::new(AnalyticsEventKind::TradeDelta, ts, &trade.symbol)
                .with_side(trade.aggressor.label())
                .with_price(trade.price_ticks)
                .with_quantity(trade.quantity_ticks),
        );

        // Large trade.
        if let Some(ev) = self.large_trades.check(trade) {
            self.large_trade_count += 1;
            self.flow.record_large_trade(trade.price_ticks);
            // Update heatmap large_trade_volume at this price.
            self.heatmap.on_large_trade(trade);
            out.events.push(ev);
        }

        // Clusters → sweeps.
        if let Some(cluster) = self.clusters.on_trade(trade) {
            out.events.push(
                AnalyticsEvent::new(AnalyticsEventKind::Cluster, cluster.end_ms, &trade.symbol)
                    .with_side(cluster.dominant_side.label())
                    .with_price(cluster.last_price)
                    .with_quantity(cluster.total_volume)
                    .with_detail(serde_json::json!({
                        "start_ms": cluster.start_ms,
                        "end_ms": cluster.end_ms,
                        "levels_crossed": cluster.levels_crossed,
                        "trade_count": cluster.trade_count,
                        "buy_volume": cluster.buy_volume,
                        "sell_volume": cluster.sell_volume,
                        "delta": cluster.delta,
                        "monotonic": cluster.monotonic,
                    })),
            );
            if let Some(ev) = self.sweeps.on_cluster(&cluster) {
                self.sweep_candidate_count += 1;
                // Map sweep event into heatmap cell.
                if let Some(price) = ev.price {
                    self.heatmap.on_sweep(price, ev.ts_ms);
                }
                out.events.push(ev);
            }
        }

        // Absorption (needs the shadow book).
        if self.book.is_ready() && !self.book.is_crossed() {
            let (best_price, opposing_liquidity) = match trade.aggressor {
                AggressorSide::Buy => (
                    self.book.best_bid(),
                    self.book.depth_volume(self.cfg.imbalance_depth).1,
                ),
                AggressorSide::Sell => (
                    self.book.best_ask(),
                    self.book.depth_volume(self.cfg.imbalance_depth).0,
                ),
            };
            if let Some(best) = best_price {
                if let Some(ev) = self.absorption.on_trade(trade, best, opposing_liquidity) {
                    self.absorption_candidate_count += 1;
                    // Map absorption event into heatmap cell.
                    if let Some(price) = ev.price {
                        self.heatmap.on_absorption(price, ev.ts_ms);
                    }
                    out.events.push(ev);
                }
            }
        }

        self.maybe_finalize_snapshot(out, ts);
    }

    /// Finalize a snapshot when a snapshot-interval boundary is crossed.
    pub fn maybe_finalize_snapshot(&mut self, out: &mut EngineOutput, ts: u64) {
        let interval = self.cfg.snapshot_interval_ms.max(1);
        let needs = match self.last_snapshot_ts {
            None => true,
            Some(last) => ts.saturating_sub(last) >= interval,
        };
        if needs {
            self.last_snapshot_ts = Some(ts);
            out.snapshots.push(self.build_snapshot(ts));
        }
    }

    /// Force-finalize a snapshot (used at end of run for both live and replay).
    pub fn force_snapshot(&mut self) -> Option<MarketMicrostructureSnapshot> {
        let ts = self.last_event_ts?;
        self.last_snapshot_ts = Some(ts);
        Some(self.build_snapshot(ts))
    }

    /// Build and reset interval counters.
    fn build_snapshot(&mut self, ts: u64) -> MarketMicrostructureSnapshot {
        let book = &self.book;
        let spread = book.spread_ticks();
        let microprice = book.microprice();
        let (bid_depth, ask_depth) = book.depth_volume(self.cfg.imbalance_depth);
        let snap = MarketMicrostructureSnapshot {
            symbol: "BTCUSDT".to_string(),
            timestamp_ms: ts,
            analytics_version: self.cfg.analytics_version.clone(),
            book_ready: book.is_ready(),
            best_bid: book.best_bid(),
            best_ask: book.best_ask(),
            mid_price: book.mid_price_f64(),
            spread_ticks: spread,
            microprice_num: microprice.map(|m| m.num),
            microprice_den: microprice.map(|m| m.den),
            trade_volume: self.trade_volume,
            buy_volume: self.buy_volume,
            sell_volume: self.sell_volume,
            delta: self.delta,
            cvd: self.flow.totals().cvd,
            bid_depth,
            ask_depth,
            book_imbalance: book.imbalance(self.cfg.imbalance_depth),
            liquidity_added: self.liquidity.added_ticks,
            liquidity_removed: self.liquidity.removed_ticks,
            large_trade_count: self.large_trade_count,
            sweep_candidate_count: self.sweep_candidate_count,
            absorption_candidate_count: self.absorption_candidate_count,
            replenishment_count: self.replenishment_count,
            book_crossed: self.book_crossed || book.is_crossed(),
            anomalies: self.anomalies,
        };

        // Reset interval counters.
        self.trade_volume = 0;
        self.buy_volume = 0;
        self.sell_volume = 0;
        self.delta = 0;
        self.large_trade_count = 0;
        self.sweep_candidate_count = 0;
        self.absorption_candidate_count = 0;
        self.replenishment_count = 0;
        self.anomalies = 0;
        self.book_crossed = false;
        self.liquidity.reset_interval();
        snap
    }

    /// Deterministic digest of the session flow analytics.
    pub fn digest(&self) -> AnalyticsFlowDigest {
        AnalyticsFlowDigest {
            large_trade_count: self.large_trades.count,
            sweep_candidate_count: self.sweeps.count,
            absorption_candidate_count: self.absorption.count,
            replenishment_count: self.liquidity.replenishment_count,
            liquidity_added: self.liquidity.added_ticks,
            liquidity_removed: self.liquidity.removed_ticks,
            ..self.flow.digest()
        }
    }

    /// Deterministic heatmap digest for live/replay comparison.
    pub fn heatmap_digest(&self) -> crate::analytics::heatmap::HeatmapDigest {
        self.heatmap.digest()
    }

    fn ts_of_ev(&self, ev: &MarketEvent) -> u64 {
        match ev {
            MarketEvent::Trade(t) => t.trade_time,
            MarketEvent::OrderBookUpdated { event_time_ms, .. } => *event_time_ms,
            _ => self.last_event_ts.unwrap_or(0),
        }
    }

    fn anomaly_event(&self, update_id: u64, symbol: &str, reason: &str) -> AnalyticsEvent {
        AnalyticsEvent::new(AnalyticsEventKind::BookAnomaly, update_id, symbol)
            .with_detail(serde_json::json!({ "reason": reason }))
    }

    /// Prune time-bounded state (retention window).
    fn prune(&mut self, now_ms: u64) {
        self.heatmap.prune(now_ms);
        self.clusters.prune(now_ms);
        self.liquidity.prune(now_ms);
        self.flow.prune(now_ms);
    }

    /// Last processed event timestamp (used for final flush timestamps).
    pub fn last_event_ts(&self) -> Option<u64> {
        self.last_event_ts
    }

    pub fn flow(&self) -> &TradeFlow {
        &self.flow
    }

    pub fn flow_mut(&mut self) -> &mut TradeFlow {
        &mut self.flow
    }

    pub fn liquidity_tracker(&self) -> &LiquidityTracker {
        &self.liquidity
    }

    pub fn large_trade_detector(&self) -> &LargeTradeDetector {
        &self.large_trades
    }

    pub fn heatmap(&self) -> &Heatmap {
        &self.heatmap
    }
}

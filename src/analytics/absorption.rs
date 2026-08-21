//! Absorption candidate detection.
//!
//! Absorption is the capacity of the book to absorb aggressive flow without
//! the price moving. The detection is **conservative**: a simple, evidence-
//! backed heuristic with a confidence score, **not** a claim about hidden
//! order flow or a full event-study.
//!
//! ## Detection
//!
//! On every aggressive trade arrival at price `P`, if:
//!
//! 1. the price window for `P` is new (or `P` differs from the reference),
//!    capture the current reference best price `ref` and mark the window
//!    open;
//! 2. within `absorption_window_ms`, the window accumulates at least
//!    `absorption_min_trades` trades and at least `absorption_min_volume`
//!    aggressive volume;
//! 3. the price fails to move beyond `absorption_max_price_excursion_ticks`
//!    against the aggressor (max favorable displacement <= excursion),
//!    i.e. the flow was absorbed;
//!
//! then a `SweepCandidate`/`AbsorptionCandidate` event is emitted.
//!
//! A single large trade (e.g. 50 BTC) never triggers absorption on its own:
//! `trade_count >= min_trades` gates the candidate.
//!
//! ## Confidence formula
//!
//! ```text
//! aggressive_volume_score = min(1, volume / min_volume)
//! price_failure_score     = 1 - min(1, max_favorable_displacement / excursion)
//! replenishment_score     = replenished_volume / (replenished_volume + consumed)
//! liquidity_persistence   = min(1, opposing_liquidity_at_window_close / opposing_liquidity_at_window_open)
//!
//! confidence = 0.30*aggressive_volume_score + 0.35*price_failure_score + 0.20*replenishment_score + 0.15*liquidity_persistence_score
//! ```

use std::collections::BTreeMap;

use crate::analytics::config::AnalyticsConfig;
use crate::analytics::events::{AnalyticsEvent, AnalyticsEventKind};
use crate::trades::trade::{AggressorSide, TradeEvent};

/// An absorption window in progress.
#[derive(Debug, Clone, PartialEq)]
pub struct AbsorptionWindow {
    pub price: u64,
    pub side: AggressorSide,
    pub open_ms: u64,
    pub close_ms: u64,
    /// Reference best price captured when the window opened.
    pub reference_price: u64,
    pub trade_count: u64,
    pub aggressive_volume: u64,
    /// Max favorable displacement in ticks (price moved toward the aggressor).
    pub max_favorable_displacement: u64,
    /// Opposing (passive) liquidity at window open.
    pub opposing_liquidity_open: u64,
    /// Opposing (passive) liquidity at window close.
    pub opposing_liquidity_close: u64,
    /// Volume replenished at the reference level during the window.
    pub replenished_volume: u64,
    pub evaluated: bool,
}

/// Absorption detector — keyed by aggressor price level.
pub struct AbsorptionDetector {
    cfg: AnalyticsConfig,
    windows: BTreeMap<u64, AbsorptionWindow>,
    pub count: u64,
}

impl AbsorptionDetector {
    pub fn new(cfg: &AnalyticsConfig) -> Self {
        Self {
            cfg: cfg.clone(),
            windows: BTreeMap::new(),
            count: 0,
        }
    }

    /// Process an aggressive trade arrival with the current book context.
    ///
    /// * `trade` — the aggressive trade.
    /// * `best_price` — current best price on the passive side (bid for BUY
    ///   aggressor, ask for SELL aggressor) in ticks.
    /// * `opposing_liquidity` — displayed liquidity on the opposing side of
    ///   the aggressor (ask size for a BUY aggressor, bid size for SELL).
    ///
    /// Returns the candidate event when the window closes.
    pub fn on_trade(
        &mut self,
        trade: &TradeEvent,
        best_price: u64,
        opposing_liquidity: u64,
    ) -> Option<AnalyticsEvent> {
        let ts = trade.trade_time;
        let p = trade.price_ticks;
        let window_ms = self.cfg.absorption_window_ms;

        let mut to_evaluate: Option<AbsorptionWindow> = None;

        match self.windows.get_mut(&p) {
            Some(w) => {
                let expired = ts.saturating_sub(w.open_ms) > window_ms;
                if expired {
                    to_evaluate = Some(self.windows.remove(&p).unwrap());
                } else {
                    w.trade_count += 1;
                    w.aggressive_volume += trade.quantity_ticks;
                    let displacement = match trade.aggressor {
                        AggressorSide::Buy => best_price.saturating_sub(w.reference_price),
                        AggressorSide::Sell => w.reference_price.saturating_sub(best_price),
                    };
                    w.max_favorable_displacement = w.max_favorable_displacement.max(displacement);
                    w.close_ms = ts;
                }
            }
            None => {
                self.windows.insert(
                    p,
                    AbsorptionWindow {
                        price: p,
                        side: trade.aggressor,
                        open_ms: ts,
                        close_ms: ts,
                        reference_price: best_price,
                        trade_count: 1,
                        aggressive_volume: trade.quantity_ticks,
                        max_favorable_displacement: 0,
                        opposing_liquidity_open: opposing_liquidity,
                        opposing_liquidity_close: opposing_liquidity,
                        replenished_volume: 0,
                        evaluated: false,
                    },
                );
            }
        }

        if let Some(w) = to_evaluate {
            return self.evaluate(&w);
        }
        None
    }

    /// Record replenishment at a passive-side level for the open windows.
    pub fn on_liquidity_change(&mut self, price: u64, delta_ticks: u64, ts: u64) {
        let window_ms = self.cfg.absorption_window_ms;
        for w in self.windows.values_mut() {
            let is_opposing = if w.side == AggressorSide::Buy {
                price > w.reference_price
            } else {
                price < w.reference_price
            };
            if is_opposing && ts.saturating_sub(w.open_ms) <= window_ms {
                w.replenished_volume += delta_ticks;
            }
        }
    }

    /// Update opposing liquidity for open windows.
    pub fn on_opposing_liquidity(&mut self, price: u64, qty: u64, ts: u64) {
        let window_ms = self.cfg.absorption_window_ms;
        for w in self.windows.values_mut() {
            let is_opposing = if w.side == AggressorSide::Buy {
                price > w.reference_price
            } else {
                price < w.reference_price
            };
            if is_opposing && ts.saturating_sub(w.open_ms) <= window_ms {
                w.opposing_liquidity_close = qty;
            }
        }
    }

    /// Force-evaluate all open windows (used on flush). Returns candidate
    /// events for windows that qualify.
    pub fn flush(&mut self) -> Vec<AnalyticsEvent> {
        let mut out = Vec::new();
        let keys: Vec<u64> = self.windows.keys().copied().collect();
        for k in keys {
            if let Some(w) = self.windows.remove(&k) {
                if let Some(ev) = self.evaluate(&w) {
                    out.push(ev);
                }
            }
        }
        out
    }

    fn evaluate(&mut self, w: &AbsorptionWindow) -> Option<AnalyticsEvent> {
        let cfg = &self.cfg;

        // Gates: min trades, min volume, no single-trade trigger.
        if (w.trade_count as u32) < cfg.absorption_min_trades {
            return None;
        }
        if w.aggressive_volume < cfg.absorption_min_volume_ticks {
            return None;
        }
        // Price excursion gate (displacement is raw ticks → normalize to tick
        // units to compare against the tick-size-unit configuration).
        let excursion = cfg.absorption_max_excursion_ticks.max(1);
        let displacement_ticks = w.max_favorable_displacement / cfg.tick_size_ticks.max(1);
        if displacement_ticks > excursion {
            return None;
        }

        let aggressive_volume_score = AnalyticsConfig::clamp_score(
            w.aggressive_volume as f64 / cfg.absorption_min_volume_ticks as f64,
        );
        let price_failure_score =
            1.0 - AnalyticsConfig::clamp_score(displacement_ticks as f64 / excursion as f64);
        let denominator = w.replenished_volume + w.aggressive_volume;
        let replenishment_score = if denominator == 0 {
            0.0
        } else {
            AnalyticsConfig::clamp_score(w.replenished_volume as f64 / denominator as f64)
        };
        let liquidity_persistence_score = if w.opposing_liquidity_open == 0 {
            1.0
        } else {
            AnalyticsConfig::clamp_score(
                w.opposing_liquidity_close as f64 / w.opposing_liquidity_open as f64,
            )
        };
        let confidence = 0.30 * aggressive_volume_score
            + 0.35 * price_failure_score
            + 0.20 * replenishment_score
            + 0.15 * liquidity_persistence_score;

        if confidence < cfg.confidence_threshold {
            return None;
        }

        self.count += 1;
        Some(
            AnalyticsEvent::new(
                AnalyticsEventKind::AbsorptionCandidate,
                w.close_ms,
                "BTCUSDT",
            )
            .with_side(w.side.label())
            .with_price(w.price)
            .with_quantity(w.aggressive_volume)
            .with_detail(serde_json::json!({
                "confidence": confidence,
                "trade_count": w.trade_count,
                "aggressive_volume_ticks": w.aggressive_volume,
                "max_favorable_displacement_ticks": w.max_favorable_displacement,
                "reference_price": w.reference_price,
                "window_open_ms": w.open_ms,
                "window_close_ms": w.close_ms,
                "opposing_liquidity_open": w.opposing_liquidity_open,
                "opposing_liquidity_close": w.opposing_liquidity_close,
                "replenished_volume_ticks": w.replenished_volume,
            })),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trades::trade::TradeEvent;

    fn trade(id: u64, price: u64, qty: u64, side: AggressorSide, ts: u64) -> TradeEvent {
        TradeEvent {
            symbol: "BTCUSDT".to_string(),
            trade_id: id,
            price_ticks: price,
            quantity_ticks: qty,
            event_time: ts,
            trade_time: ts,
            local_receive_time_ns: 0,
            aggressor: side,
            order_type: "MARKET".to_string(),
        }
    }

    #[test]
    fn test_absorption_detected() {
        let cfg = AnalyticsConfig::btcusdt_default();
        let mut det = AbsorptionDetector::new(&cfg);
        // Aggressive buys at 68000 while best bid stays at 68000 (no move).
        let p = 6_800_000_000_000u64;
        let big = 500_000_000; // 5 BTC
                               // Window: 5 trades * 5 BTC = 25 BTC >= 20 BTC min.
        for i in 0..5 {
            let t = trade(i, p, big, AggressorSide::Buy, 1000 + i * 5);
            det.on_trade(&t, p, 10_000_000_000);
        }
        let events = det.flush();
        assert!(events
            .iter()
            .any(|e| e.kind == AnalyticsEventKind::AbsorptionCandidate));
        assert_eq!(det.count, 1);
    }

    #[test]
    fn test_single_large_trade_no_trigger() {
        let cfg = AnalyticsConfig::btcusdt_default();
        let mut det = AbsorptionDetector::new(&cfg);
        // A single 50 BTC trade (many times the min volume) but < min trades.
        let p = 6_800_000_000_000u64;
        let t = trade(1, p, 5_000_000_000, AggressorSide::Buy, 1000);
        assert!(det.on_trade(&t, p, 10_000_000_000).is_none());
        assert_eq!(det.count, 0);
    }

    #[test]
    fn test_displacement_exceeds_excursion() {
        let cfg = AnalyticsConfig::btcusdt_default();
        let mut det = AbsorptionDetector::new(&cfg);
        let p = 6_800_000_000_000u64;
        let big = 500_000_000;
        // Reference best bid captured at p; best bid moves up 10 ticks (beyond
        // the 3-tick excursion) → flow was NOT absorbed.
        for i in 0..5 {
            let t = trade(i, p, big, AggressorSide::Buy, 1000 + i * 5);
            let best = p + 10_000_000 * (i + 1);
            det.on_trade(&t, best, 10_000_000_000);
        }
        assert_eq!(det.count, 0);
    }
}

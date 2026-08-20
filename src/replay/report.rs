//! Text report for a replay run.

use std::fmt;

use crate::recording::session::SessionRecord;
use crate::replay::engine::ReplayOutcome;

pub struct ReplayReport {
    pub session: SessionRecord,
    pub outcome: ReplayOutcome,
}

impl fmt::Display for ReplayReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let o = &self.outcome;
        writeln!(
            f,
            "┌──────────────────────────────────────────────────────┐"
        )?;
        writeln!(
            f,
            "│                    REPLAY REPORT                     │"
        )?;
        writeln!(
            f,
            "└──────────────────────────────────────────────────────┘"
        )?;
        writeln!(f, "Session:        {}", self.session.session_id)?;
        writeln!(f, "Symbol:         {}", self.session.symbol)?;
        writeln!(f, "Status:         {}", self.session.status)?;
        writeln!(f, "Recorded at:    {}", self.session.started_at)?;
        writeln!(f, "Duration:       {:.3}s", self.session.duration_secs())?;
        writeln!(f, "  events total:        {}", o.events_total)?;
        writeln!(f, "  depth events:        {}", o.depth_events)?;
        writeln!(f, "  snapshots applied:   {}", o.snapshots_applied)?;
        writeln!(f, "  depth applied:       {}", o.events_applied)?;
        writeln!(f, "  depth ignored:       {}", o.events_ignored)?;
        writeln!(f, "  sequence errors:     {}", o.sequence_errors)?;
        writeln!(f, "  trades processed:    {}", o.trades_processed)?;
        writeln!(f, "  trades skipped:      {}", o.trades_skipped)?;
        writeln!(
            f,
            "Replay wall time:     {:.3}s",
            o.duration_ns as f64 / 1e9
        )?;
        writeln!(f, "Final book state:")?;
        writeln!(f, "  last_update_id:      {}", o.final_update_id)?;
        writeln!(f, "  bid levels:          {}", o.book_bid_levels)?;
        writeln!(f, "  ask levels:          {}", o.book_ask_levels)?;
        match (o.best_bid, o.best_ask) {
            (Some(b), Some(a)) => {
                writeln!(
                    f,
                    "  best bid:            {}",
                    crate::orderbook::level::ticks_to_price_str(b)
                )?;
                writeln!(
                    f,
                    "  best ask:            {}",
                    crate::orderbook::level::ticks_to_price_str(a)
                )?;
                writeln!(
                    f,
                    "  spread:              {}",
                    crate::orderbook::level::ticks_to_price_str(a - b)
                )?;
            }
            (b, a) => {
                writeln!(f, "  best bid:            {:?}", b)?;
                writeln!(f, "  best ask:            {:?}", a)?;
            }
        }
        writeln!(
            f,
            "  mid price:           {}",
            o.mid_price
                .map(|m| format!("{:.2}", m))
                .unwrap_or_else(|| "n/a".to_string())
        )?;
        Ok(())
    }
}

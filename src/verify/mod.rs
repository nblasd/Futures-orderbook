//! Phase 3 verification: audit a recorded session for data-quality issues.

use std::collections::{HashMap, HashSet};
use std::fmt;

use uuid::Uuid;

use crate::recording::session::SessionRecord;
use crate::storage::Storage;

/// Result of a verification pass over a recorded session.
#[derive(Debug, Clone, Default)]
pub struct VerifyReport {
    pub session: Option<SessionRecord>,
    pub raw_events: u64,
    pub raw_parse_errors: u64,
    pub trade_rows: u64,
    pub level_change_rows: u64,
    pub depth_events: u64,
    pub snapshots: u64,
    /// trade_ids that appear more than once in the trades table.
    pub duplicate_trade_ids: Vec<u64>,
    /// Number of level-change rows sharing the same (final_update_id, side, price).
    pub duplicate_depth_identities: u64,
    /// trade_ids of rows that look like synthetic markers (zero price/qty).
    pub marker_as_trade_ids: Vec<u64>,
    /// Rows with an invalid symbol (mismatching the session symbol).
    pub invalid_symbol_rows: u64,
    /// Depth events whose final_update_id does not strictly increase by seq.
    pub depth_sequence_anomalies: u64,
    /// True when no issues were found.
    pub verified: bool,
}

impl VerifyReport {
    pub fn issues(&self) -> usize {
        self.raw_parse_errors as usize
            + self.duplicate_trade_ids.len()
            + self.duplicate_depth_identities as usize
            + self.marker_as_trade_ids.len()
            + self.invalid_symbol_rows as usize
            + self.depth_sequence_anomalies as usize
    }
}

impl fmt::Display for VerifyReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "┌──────────────────────────────────────────────────────┐"
        )?;
        writeln!(f, "│                   VERIFY REPORT                     │")?;
        writeln!(
            f,
            "└──────────────────────────────────────────────────────┘"
        )?;
        if let Some(s) = &self.session {
            writeln!(f, "Session:        {}", s.session_id)?;
            writeln!(f, "Symbol:         {}", s.symbol)?;
            writeln!(f, "Status:         {}", s.status)?;
        }
        writeln!(f, "Raw events:          {}", self.raw_events)?;
        writeln!(f, "  parse errors:      {}", self.raw_parse_errors)?;
        writeln!(f, "Trade rows:          {}", self.trade_rows)?;
        writeln!(f, "Level-change rows:   {}", self.level_change_rows)?;
        writeln!(f, "Depth events:        {}", self.depth_events)?;
        writeln!(f, "Snapshots:           {}", self.snapshots)?;
        writeln!(f, "Duplicate trade IDs: {}", self.duplicate_trade_ids.len())?;
        if !self.duplicate_trade_ids.is_empty() {
            let shown: Vec<String> = self
                .duplicate_trade_ids
                .iter()
                .take(10)
                .map(|id| id.to_string())
                .collect();
            writeln!(f, "  (first {}: {})", shown.len(), shown.join(", "))?;
        }
        writeln!(
            f,
            "Duplicate depth identities: {}",
            self.duplicate_depth_identities
        )?;
        writeln!(
            f,
            "Marker-as-trade rows:       {}",
            self.marker_as_trade_ids.len()
        )?;
        writeln!(
            f,
            "Invalid-symbol rows:        {}",
            self.invalid_symbol_rows
        )?;
        writeln!(
            f,
            "Depth sequence anomalies:   {}",
            self.depth_sequence_anomalies
        )?;
        writeln!(f, "──────────────────────────────────────────────────────")?;
        let issues = self.issues();
        if issues == 0 {
            writeln!(f, "VERIFIED: no data-quality issues found")?;
        } else {
            writeln!(f, "FAILED: {} data-quality issue(s) found", issues)?;
        }
        Ok(())
    }
}

/// Audit a recorded session for data-quality issues.
pub async fn verify_session(
    storage: &dyn Storage,
    session_id: Uuid,
) -> anyhow::Result<VerifyReport> {
    let mut report = VerifyReport {
        session: storage.get_session(session_id).await?,
        ..VerifyReport::default()
    };

    let expected_symbol = report.session.as_ref().map(|s| s.symbol.clone());

    // --- Raw events ---
    let raw = storage.read_raw_events(session_id).await?;
    report.raw_events = raw.len() as u64;
    for row in &raw {
        if serde_json::from_str::<serde_json::Value>(&row.raw_payload).is_err() {
            report.raw_parse_errors += 1;
        }
    }

    // --- Trades ---
    let trades = storage.read_trades(session_id).await?;
    report.trade_rows = trades.len() as u64;
    let mut seen_trade_ids: HashMap<u64, usize> = HashMap::new();
    for t in &trades {
        *seen_trade_ids.entry(t.trade_id).or_insert(0) += 1;
        if t.price == 0 || t.quantity == 0 || t.order_type == "NA" {
            report.marker_as_trade_ids.push(t.trade_id);
        }
        if let Some(sym) = &expected_symbol {
            if &t.symbol != sym {
                report.invalid_symbol_rows += 1;
            }
        }
    }
    report.duplicate_trade_ids = seen_trade_ids
        .into_iter()
        .filter(|(_, c)| *c > 1)
        .map(|(id, _)| id)
        .collect();
    report.duplicate_trade_ids.sort_unstable();

    // --- Depth ---
    let levels = storage.read_level_changes(session_id).await?;
    report.level_change_rows = levels.len() as u64;

    let mut identities: HashSet<(u64, &str, u64)> = HashSet::new();
    for r in &levels {
        if !identities.insert((r.final_update_id, r.side.as_str(), r.price)) {
            report.duplicate_depth_identities += 1;
        }
        if let Some(sym) = &expected_symbol {
            if &r.symbol != sym {
                report.invalid_symbol_rows += 1;
            }
        }
    }

    // Sequence check: group rows by seq, then verify final_update_id strictly
    // increases from one event to the next.
    report.depth_events = storage.count_depth_events(session_id).await?;
    let mut prev_final: Option<u64> = None;
    let mut i = 0;
    while i < levels.len() {
        let seq = levels[i].seq;
        let mut event_final: Option<u64> = None;
        while i < levels.len() && levels[i].seq == seq {
            let f = levels[i].final_update_id;
            event_final = Some(match event_final {
                Some(prev) => prev.max(f),
                None => f,
            });
            i += 1;
        }
        if let (Some(f), Some(prev)) = (event_final, prev_final) {
            if f <= prev {
                report.depth_sequence_anomalies += 1;
            }
        }
        if let Some(f) = event_final {
            prev_final = Some(f);
        }
    }

    report.snapshots = storage.read_snapshots(session_id).await?.len() as u64;
    report.verified = report.issues() == 0;

    Ok(report)
}

/// Human-readable storage digest for the verify summary (counts only).
pub async fn session_counts(
    storage: &dyn Storage,
    session_id: Uuid,
) -> anyhow::Result<(u64, u64, u64)> {
    Ok((
        storage.count_raw_events(session_id).await?,
        storage.count_trades(session_id).await?,
        storage.count_level_changes(session_id).await?,
    ))
}

/// Format a `DateTime` for reporting.
pub fn fmt_ms(ms: u64) -> String {
    crate::storage::ms_to_datetime(ms)
        .format("%Y-%m-%d %H:%M:%S%.3f UTC")
        .to_string()
}

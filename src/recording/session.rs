use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Possible recording session statuses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionStatus {
    /// Session row created but ingestion not yet running.
    Starting,
    /// Actively recording.
    Recording,
    /// Recording but storage has degraded (queue overflow, insert failures).
    Degraded,
    /// Shutdown in progress, final flush pending.
    Stopping,
    /// Final flush succeeded; session closed cleanly.
    Completed,
    /// Recording failed (storage failure, fatal error).
    Failed,
}

impl SessionStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            SessionStatus::Starting => "STARTING",
            SessionStatus::Recording => "RECORDING",
            SessionStatus::Degraded => "DEGRADED",
            SessionStatus::Stopping => "STOPPING",
            SessionStatus::Completed => "COMPLETED",
            SessionStatus::Failed => "FAILED",
        }
    }
}

impl std::fmt::Display for SessionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// One recording-process lifetime record.
#[derive(Debug, Clone, Serialize, Deserialize, ::clickhouse::Row)]
pub struct SessionRecord {
    #[serde(with = "::clickhouse::serde::uuid")]
    pub session_id: Uuid,
    pub exchange: String,
    pub market_type: String,
    pub symbol: String,
    pub contract_type: String,
    #[serde(with = "::clickhouse::serde::chrono::datetime64::millis")]
    pub started_at: DateTime<Utc>,
    #[serde(with = "::clickhouse::serde::chrono::datetime64::millis::option")]
    pub ended_at: Option<DateTime<Utc>>,
    pub software_version: String,
    pub git_commit: String,
    pub depth_stream: String,
    pub trade_stream: String,
    pub status: String,
}

impl SessionRecord {
    /// Build a new session record in STARTING state.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        symbol: &str,
        exchange: &str,
        market_type: &str,
        contract_type: &str,
        depth_stream: &str,
        trade_stream: &str,
        software_version: &str,
        git_commit: &str,
    ) -> Self {
        Self {
            session_id: Uuid::new_v4(),
            exchange: exchange.to_string(),
            market_type: market_type.to_string(),
            symbol: symbol.to_string(),
            contract_type: contract_type.to_string(),
            started_at: Utc::now(),
            ended_at: None,
            software_version: software_version.to_string(),
            git_commit: git_commit.to_string(),
            depth_stream: depth_stream.to_string(),
            trade_stream: trade_stream.to_string(),
            status: SessionStatus::Starting.as_str().to_string(),
        }
    }

    /// Session duration in seconds (from start to end, or now if still open).
    pub fn duration_secs(&self) -> f64 {
        let end = self.ended_at.unwrap_or_else(Utc::now);
        (end - self.started_at).num_milliseconds() as f64 / 1000.0
    }
}

/// Best-effort detection of the current git commit.
pub fn detect_git_commit() -> String {
    match std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
    {
        Ok(out) if out.status.success() => String::from_utf8_lossy(&out.stdout).trim().to_string(),
        _ => "unknown".to_string(),
    }
}

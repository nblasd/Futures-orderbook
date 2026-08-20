//! Recording metrics and storage health shared between the recorder
//! (producer) and the storage worker (consumer).

/// Overall storage health reflected in diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StorageState {
    #[default]
    Connecting,
    Healthy,
    Degraded,
    Failed,
}

impl StorageState {
    pub fn label(self) -> &'static str {
        match self {
            StorageState::Connecting => "CONNECTING",
            StorageState::Healthy => "CONNECTED",
            StorageState::Degraded => "DEGRADED",
            StorageState::Failed => "FAILED",
        }
    }
}

/// Shared storage health signal. `degraded` is set by the recorder on queue
/// overflow and by the worker on insert failures; the worker syncs it to the
/// session status (DEGRADED ↔ RECORDING) without blocking the event loop.
#[derive(Debug, Clone, Default)]
pub struct StorageHealth {
    pub state: StorageState,
    pub degraded: bool,
    pub last_error: Option<String>,
    pub queue_overflows: u64,
    pub insert_failures: u64,
}

impl StorageHealth {
    pub fn new() -> Self {
        Self {
            state: StorageState::Connecting,
            degraded: false,
            last_error: None,
            queue_overflows: 0,
            insert_failures: 0,
        }
    }
}

/// Counters tracked by the recorder + storage worker.
#[derive(Debug, Clone, Default)]
pub struct RecorderMetrics {
    // Raw events
    pub raw_received: u64,
    pub raw_bytes_received: u64,
    pub raw_stored: u64,
    pub raw_bytes_stored: u64,
    pub raw_dropped: u64,
    // Trades
    pub trades_received: u64,
    pub trades_stored: u64,
    pub trades_dropped: u64,
    pub marker_rejected: u64,
    pub invalid_rejected: u64,
    // Depth
    pub depth_events_received: u64,
    pub level_changes_stored: u64,
    pub depth_dropped: u64,
    pub snapshots_stored: u64,
    // Storage
    pub batches_flushed: u64,
    pub failed_batches: u64,
    /// Number of individual insert attempts that failed and were retried.
    pub retries: u64,
    pub queue_depth: usize,
    pub queue_high_water: usize,
    pub last_flush_duration_ns: u128,
    pub max_flush_duration_ns: u128,
    pub total_flush_duration_ns: u128,
    pub flush_ops: u64,
    pub queue_capacity: usize,
}

impl RecorderMetrics {
    pub fn avg_flush_duration_ns(&self) -> u128 {
        if self.flush_ops == 0 {
            0
        } else {
            self.total_flush_duration_ns / self.flush_ops as u128
        }
    }

    pub fn record_flush(&mut self, duration_ns: u128) {
        self.flush_ops += 1;
        self.total_flush_duration_ns += duration_ns;
        self.last_flush_duration_ns = duration_ns;
        if duration_ns > self.max_flush_duration_ns {
            self.max_flush_duration_ns = duration_ns;
        }
    }
}

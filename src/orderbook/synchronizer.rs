use std::collections::VecDeque;

use crate::binance::types::DepthUpdate;
use tracing::{debug, error, info, warn};

/// Synchronization state machine for the local order book.
///
/// States and transitions:
///
/// ```text
/// Disconnected
///     ↓
/// Connecting
///     ↓
/// Buffering
///     ↓
/// SnapshotLoading
///     ↓
/// Synchronizing
///     ↓
/// Ready
///
/// Failure path:
/// Ready
///   ↓
/// SequenceGap / PuMismatch
///   ↓
/// ResyncRequired
///   ↓
/// Buffering
///   ↓
/// SnapshotLoading
///   ↓
/// Synchronizing
///   ↓
/// Ready
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncState {
    /// Not connected to WebSocket.
    Disconnected,
    /// WebSocket connection in progress.
    Connecting,
    /// Receiving and buffering depth events, waiting for snapshot.
    Buffering,
    /// REST snapshot request in flight.
    SnapshotLoading,
    /// Snapshot received; reconciling buffered events.
    Synchronizing,
    /// Book is fully synchronized and live.
    Ready,
    /// A sequence gap was detected; resync needed.
    ResyncRequired,
    /// WebSocket reconnection in progress.
    Reconnecting,
    /// Engine is shutting down.
    Stopping,
}

/// The synchronizer manages the state machine that keeps the local order book
/// in sync with the Binance Futures WebSocket depth stream.
///
/// ## Synchronization Invariant (from Binance Futures docs):
///
/// 1. Open WebSocket, buffer depth events.
/// 2. Fetch REST snapshot → `lastUpdateId`.
/// 3. Discard buffered events where `u < lastUpdateId`.
/// 4. The first processed event must satisfy:
///    `U <= lastUpdateId AND u >= lastUpdateId`
/// 5. Each subsequent event's `pu` must equal the previous event's `u`.
///    If not → invalidate and resync.
///
/// The `pu` field is Binance Futures-specific and is NOT available on Spot.
/// It provides an additional continuity check: `pu` of event N must equal
/// `u` of event N-1.
pub struct Synchronizer {
    state: SyncState,
    /// Buffered depth events received before/during snapshot fetch.
    buffer: VecDeque<DepthUpdate>,
    /// The lastUpdateId from the most recent REST snapshot.
    snapshot_last_update_id: u64,
    /// The `u` value of the last applied event (for `pu` continuity check).
    last_applied_u: Option<u64>,
    /// Number of events received since last connect/sync.
    events_received: u64,
    /// Number of events successfully applied to the book.
    events_applied: u64,
    /// Number of events ignored (stale, duplicate).
    events_ignored: u64,
    /// Number of sequence errors.
    sequence_errors: u64,
    /// Number of resync operations.
    resync_count: u64,
    /// Number of reconnections.
    reconnect_count: u64,
}

impl Synchronizer {
    pub fn new() -> Self {
        Self {
            state: SyncState::Disconnected,
            buffer: VecDeque::new(),
            snapshot_last_update_id: 0,
            last_applied_u: None,
            events_received: 0,
            events_applied: 0,
            events_ignored: 0,
            sequence_errors: 0,
            resync_count: 0,
            reconnect_count: 0,
        }
    }

    /// Get the current synchronization state.
    pub fn state(&self) -> SyncState {
        self.state
    }

    /// Transition to Connecting state.
    pub fn on_connecting(&mut self) {
        info!("Connecting to Binance Futures");
        self.state = SyncState::Connecting;
    }

    /// Transition to Buffering state after WebSocket connects.
    pub fn on_connected(&mut self) {
        info!("WebSocket connected, subscribing to depth stream");
        self.state = SyncState::Buffering;
        self.buffer.clear();
        self.last_applied_u = None;
        self.events_received = 0;
        self.events_applied = 0;
        self.events_ignored = 0;
        self.sequence_errors = 0;
    }

    /// Transition to Reconnecting state.
    pub fn on_reconnecting(&mut self) {
        warn!("WebSocket disconnected, reconnecting");
        self.state = SyncState::Reconnecting;
        self.reconnect_count += 1;
    }

    /// Transition to SnapshotLoading state.
    pub fn on_snapshot_loading(&mut self) {
        info!("Requesting Futures snapshot");
        self.state = SyncState::SnapshotLoading;
    }

    /// Process a buffered depth event while in Buffering state.
    /// Events are queued for later reconciliation with the snapshot.
    pub fn buffer_event(&mut self, event: DepthUpdate) {
        if self.state == SyncState::Buffering {
            self.events_received += 1;
            self.buffer.push_back(event);
        }
    }

    /// Process a live depth event while in Ready state.
    /// Returns Some(event) if the event should be applied, None if it should be ignored.
    pub fn process_live_event(&mut self, event: &DepthUpdate) -> ProcessResult {
        self.events_received += 1;

        match self.state {
            SyncState::Ready => {
                // Check pu continuity
                if let Some(prev_u) = self.last_applied_u {
                    if event.previous_final_update_id != prev_u {
                        error!(
                            "pu continuity failure: expected pu={}, got pu={}",
                            prev_u, event.previous_final_update_id
                        );
                        self.sequence_errors += 1;
                        return ProcessResult::PuMismatch;
                    }
                }

                // Check that this event's U is consistent with our last applied u
                if let Some(prev_u) = self.last_applied_u {
                    if event.final_update_id <= prev_u {
                        // Stale or duplicate event
                        debug!(
                            "Stale event ignored: u={} <= last_applied_u={}",
                            event.final_update_id, prev_u
                        );
                        self.events_ignored += 1;
                        return ProcessResult::Stale;
                    }
                }

                self.last_applied_u = Some(event.final_update_id);
                self.events_applied += 1;
                ProcessResult::Apply
            }
            SyncState::Buffering => {
                self.buffer_event(event.clone());
                ProcessResult::Buffered
            }
            SyncState::Synchronizing => {
                // Still processing buffered events during sync
                ProcessResult::Buffered
            }
            _ => {
                self.events_ignored += 1;
                ProcessResult::Ignored
            }
        }
    }

    /// Reconcile buffered events after receiving the REST snapshot.
    ///
    /// Implements the Binance Futures synchronization procedure:
    /// 1. Discard buffered events where `u < snapshot.lastUpdateId`
    /// 2. Find the first event where `U <= lastUpdateId AND u >= lastUpdateId`
    /// 3. Apply that event and all subsequent events in order
    ///
    /// Returns the list of events that should be applied to the book,
    /// in order, or an error if synchronization is impossible.
    pub fn reconcile(
        &mut self,
        snapshot_last_update_id: u64,
    ) -> Result<Vec<DepthUpdate>, SyncError> {
        self.snapshot_last_update_id = snapshot_last_update_id;
        info!(
            "Synchronizing with snapshot lastUpdateId={}",
            snapshot_last_update_id
        );

        // Step 1: Discard events where u < lastUpdateId
        while let Some(front) = self.buffer.front() {
            if front.final_update_id < snapshot_last_update_id {
                self.buffer.pop_front();
                self.events_ignored += 1;
            } else {
                break;
            }
        }

        // Step 2: Find the first bridging event where U <= lastUpdateId AND u >= lastUpdateId
        let mut bridge_index = None;
        for (i, event) in self.buffer.iter().enumerate() {
            if event.first_update_id <= snapshot_last_update_id
                && event.final_update_id >= snapshot_last_update_id
            {
                bridge_index = Some(i);
                break;
            }
        }

        let bridge_idx = match bridge_index {
            Some(idx) => idx,
            None => {
                warn!(
                    "No bridging event found in buffer ({} events buffered), need resync",
                    self.buffer.len()
                );
                return Err(SyncError::NoBridgingEvent);
            }
        };

        // Step 3: Extract events from the bridge point onward
        let mut events_to_apply = Vec::new();
        let mut expected_pu: Option<u64> = None;

        // The first event starts the pu chain
        if let Some(first_event) = self.buffer.get(bridge_idx) {
            expected_pu = None; // First event doesn't need pu check
            events_to_apply.push(first_event.clone());
            self.last_applied_u = Some(first_event.final_update_id);
        }

        // Verify pu continuity for subsequent buffered events
        for event in self.buffer.iter().skip(bridge_idx + 1) {
            if let Some(prev_u) = expected_pu {
                if event.previous_final_update_id != prev_u {
                    warn!(
                        "pu continuity broken in buffer: expected pu={}, got pu={}",
                        prev_u, event.previous_final_update_id
                    );
                    // Discard the rest of the buffer, the chain is broken
                    break;
                }
            }
            events_to_apply.push(event.clone());
            expected_pu = Some(event.final_update_id);
            self.last_applied_u = Some(event.final_update_id);
        }

        self.events_applied += events_to_apply.len() as u64;
        self.state = SyncState::Synchronizing;

        // Clear the buffer — all remaining events have been either
        // applied (returned to caller) or discarded (stale / pu break).
        // Retaining stale events would leak memory and confuse diagnostics.
        self.buffer.clear();

        info!(
            "Reconciled {} events from buffer, transitioning to Ready",
            events_to_apply.len()
        );
        self.state = SyncState::Ready;

        Ok(events_to_apply)
    }

    /// Trigger a resync operation. Transitions back to Buffering.
    pub fn trigger_resync(&mut self) {
        warn!("Resynchronization started");
        self.resync_count += 1;
        self.state = SyncState::Buffering;
        self.buffer.clear();
        self.last_applied_u = None;
    }

    /// Shutdown the synchronizer.
    pub fn shutdown(&mut self) {
        info!("Synchronizer shutting down");
        self.state = SyncState::Stopping;
    }

    /// Get the number of events received.
    pub fn events_received(&self) -> u64 {
        self.events_received
    }

    /// Get the number of events applied.
    pub fn events_applied(&self) -> u64 {
        self.events_applied
    }

    /// Get the number of events ignored.
    pub fn events_ignored(&self) -> u64 {
        self.events_ignored
    }

    /// Get the number of sequence errors.
    pub fn sequence_errors(&self) -> u64 {
        self.sequence_errors
    }

    /// Get the number of resyncs.
    pub fn resync_count(&self) -> u64 {
        self.resync_count
    }

    /// Get the number of reconnections.
    pub fn reconnect_count(&self) -> u64 {
        self.reconnect_count
    }

    /// Get the last applied `u` value.
    pub fn last_applied_u(&self) -> Option<u64> {
        self.last_applied_u
    }

    /// Get the snapshot's lastUpdateId.
    pub fn snapshot_last_update_id(&self) -> u64 {
        self.snapshot_last_update_id
    }

    /// Get the current buffer size.
    pub fn buffer_size(&self) -> usize {
        self.buffer.len()
    }

    /// Check if the state machine is in Ready state.
    pub fn is_ready(&self) -> bool {
        self.state == SyncState::Ready
    }
}

/// Result of processing a live depth event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessResult {
    /// Event should be applied to the book.
    Apply,
    /// Event was buffered (not in Ready state).
    Buffered,
    /// Event was stale and ignored.
    Stale,
    /// pu continuity was broken.
    PuMismatch,
    /// Event was ignored for other reasons.
    Ignored,
}

/// Errors during synchronization.
#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error("no bridging event found in buffer")]
    NoBridgingEvent,
    #[error("sequence gap detected")]
    SequenceGap,
    #[error("pu continuity failure: expected pu={expected}, got pu={got}")]
    PuMismatch { expected: u64, got: u64 },
}

impl Default for Synchronizer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binance::types::DepthUpdate;

    fn make_depth_update(u: u64, pu: u64, first_update_id: u64) -> DepthUpdate {
        DepthUpdate {
            event_type: "depthUpdate".to_string(),
            event_time: 1234567890,
            transaction_time: 1234567890,
            symbol: "BTCUSDT".to_string(),
            first_update_id,
            final_update_id: u,
            previous_final_update_id: pu,
            bids: vec![],
            asks: vec![],
        }
    }

    #[test]
    fn test_sync_starts_disconnected() {
        let sync = Synchronizer::new();
        assert_eq!(sync.state(), SyncState::Disconnected);
    }

    #[test]
    fn test_buffering_state() {
        let mut sync = Synchronizer::new();
        sync.on_connecting();
        assert_eq!(sync.state(), SyncState::Connecting);
        sync.on_connected();
        assert_eq!(sync.state(), SyncState::Buffering);
    }

    #[test]
    fn test_buffer_event() {
        let mut sync = Synchronizer::new();
        sync.on_connecting();
        sync.on_connected();

        let event = make_depth_update(101, 100, 100);
        sync.buffer_event(event.clone());
        assert_eq!(sync.buffer_size(), 1);
        assert_eq!(sync.events_received(), 1);
    }

    #[test]
    fn test_reconcile_discards_stale_events() {
        let mut sync = Synchronizer::new();
        sync.on_connecting();
        sync.on_connected();

        // Buffer events with u=98, 99, 101, 102
        sync.buffer_event(make_depth_update(98, 97, 97));
        sync.buffer_event(make_depth_update(99, 98, 98));
        sync.buffer_event(make_depth_update(101, 99, 100));
        sync.buffer_event(make_depth_update(102, 101, 101));

        // Snapshot has lastUpdateId=100
        // Events with u < 100 (i.e., u=98, u=99) should be discarded
        let events = sync.reconcile(100).unwrap();

        // First bridging event: U <= 100 AND u >= 100
        // Event u=101 has U=100 <= 100 AND u=101 >= 100 ✓
        // Event u=102 has U=101 > 100 ✗ (but it's after the bridge)
        assert!(events.len() >= 1);
        assert_eq!(events[0].final_update_id, 101);
    }

    #[test]
    fn test_reconcile_finds_correct_bridge() {
        let mut sync = Synchronizer::new();
        sync.on_connecting();
        sync.on_connected();

        // Buffer: event with U=99,u=100 and event with U=100,u=101
        sync.buffer_event(make_depth_update(100, 99, 99));
        sync.buffer_event(make_depth_update(101, 100, 100));

        // Snapshot lastUpdateId=100
        // Event u=100: U=99 <= 100 AND u=100 >= 100 ✓ (first bridge)
        let events = sync.reconcile(100).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].final_update_id, 100);
        assert_eq!(events[1].final_update_id, 101);
    }

    #[test]
    fn test_no_bridging_event_triggers_error() {
        let mut sync = Synchronizer::new();
        sync.on_connecting();
        sync.on_connected();

        // Buffer: event with U=95, u=96 (all u < 100 will be discarded)
        sync.buffer_event(make_depth_update(96, 95, 95));

        // Snapshot lastUpdateId=100
        // After discarding u < 100, buffer is empty → no bridging event
        let result = sync.reconcile(100);
        assert!(result.is_err());
    }

    #[test]
    fn test_stale_event_detected() {
        let mut sync = Synchronizer::new();
        sync.on_connecting();
        sync.on_connected();

        // Buffer events while in Buffering state
        sync.buffer_event(make_depth_update(101, 100, 100));

        sync.on_snapshot_loading();
        sync.reconcile(100).unwrap();

        // Now in Ready state, try a stale event (correct pu but u <= last_applied_u)
        // make_depth_update(u=100, pu=101, U=100) -> pu matches last_applied_u=101, but u=100 <= 101
        let stale = make_depth_update(100, 101, 100);
        let result = sync.process_live_event(&stale);
        assert_eq!(result, ProcessResult::Stale);
    }

    #[test]
    fn test_pu_continuity_failure() {
        let mut sync = Synchronizer::new();
        sync.on_connecting();
        sync.on_connected();

        // Buffer events while in Buffering state
        sync.buffer_event(make_depth_update(101, 100, 100));

        sync.on_snapshot_loading();
        sync.reconcile(100).unwrap();
        // After reconcile, state is Ready and last_applied_u is set.

        // Create event with wrong pu
        let bad_pu_event = DepthUpdate {
            event_type: "depthUpdate".to_string(),
            event_time: 1234567890,
            transaction_time: 1234567890,
            symbol: "BTCUSDT".to_string(),
            first_update_id: 102,
            final_update_id: 103,
            previous_final_update_id: 999, // Wrong pu!
            bids: vec![],
            asks: vec![],
        };

        let result = sync.process_live_event(&bad_pu_event);
        assert_eq!(result, ProcessResult::PuMismatch);
        assert_eq!(sync.sequence_errors(), 1);
    }

    #[test]
    fn test_resync_state_transition() {
        let mut sync = Synchronizer::new();
        sync.on_connecting();
        sync.on_connected();

        // Buffer events while in Buffering state
        sync.buffer_event(make_depth_update(101, 100, 100));

        sync.on_snapshot_loading();
        sync.reconcile(100).unwrap();

        assert_eq!(sync.state(), SyncState::Ready);
        sync.trigger_resync();
        assert_eq!(sync.state(), SyncState::Buffering);
        assert_eq!(sync.resync_count(), 1);
    }

    #[test]
    fn test_buffer_cleared_after_reconcile() {
        let mut sync = Synchronizer::new();
        sync.on_connecting();
        sync.on_connected();

        // Buffer many events, only a few will bridge the snapshot
        for u in 80..=110 {
            sync.buffer_event(make_depth_update(u, u - 1, u - 1));
        }
        assert_eq!(sync.buffer_size(), 31);

        // Snapshot lastUpdateId=100
        // Stale events (u < 100) discarded, bridge found, chain extracted
        let events = sync.reconcile(100).unwrap();
        assert!(events.len() >= 1);

        // The buffer must be empty after reconciliation.
        // All remaining events were either applied (returned) or discarded
        // (stale/pu break). Retaining them would leak memory and confuse
        // diagnostics.
        assert_eq!(
            sync.buffer_size(),
            0,
            "buffer should be empty after reconcile, but has {} events",
            sync.buffer_size()
        );
    }

    #[test]
    fn test_reconnect_increments_counter() {
        let mut sync = Synchronizer::new();
        sync.on_reconnecting();
        assert_eq!(sync.reconnect_count(), 1);
        sync.on_reconnecting();
        assert_eq!(sync.reconnect_count(), 2);
    }
}

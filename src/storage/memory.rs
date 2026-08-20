//! In-memory [`Storage`] implementation used by the offline end-to-end tests.
//!
//! No network, no ClickHouse — deterministic and suitable for unit/integration
//! tests. The full recorder → storage → replay pipeline runs against it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::{LevelChangeRow, RawEventRow, SnapshotRow, Storage, TradeRow};
use crate::recording::session::SessionRecord;

/// Contents of the in-memory database.
#[derive(Debug, Clone, Default)]
pub struct MemoryDb {
    pub sessions: HashMap<Uuid, SessionRecord>,
    pub raw_events: Vec<RawEventRow>,
    pub trades: Vec<TradeRow>,
    pub level_changes: Vec<LevelChangeRow>,
    pub snapshots: Vec<SnapshotRow>,
}

#[derive(Debug, Clone, Default)]
pub struct MemoryStorage {
    db: Arc<Mutex<MemoryDb>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }

    /// Cloned snapshot of the current database contents (for test assertions).
    pub fn snapshot_db(&self) -> MemoryDb {
        self.db.lock().unwrap().clone()
    }
}

fn in_session<T>(rows: &[T], session_id: Uuid, extract: impl Fn(&T) -> Uuid) -> Vec<T>
where
    T: Clone,
{
    rows.iter()
        .filter(|r| extract(r) == session_id)
        .cloned()
        .collect()
}

#[async_trait::async_trait]
impl Storage for MemoryStorage {
    async fn ping(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn init_schema(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn insert_session(&self, session: &SessionRecord) -> anyhow::Result<()> {
        self.db
            .lock()
            .unwrap()
            .sessions
            .insert(session.session_id, session.clone());
        Ok(())
    }

    async fn update_session_status(
        &self,
        session_id: Uuid,
        status: &str,
        ended_at: Option<DateTime<Utc>>,
    ) -> anyhow::Result<()> {
        let mut db = self.db.lock().unwrap();
        if let Some(s) = db.sessions.get_mut(&session_id) {
            s.status = status.to_string();
            s.ended_at = ended_at;
        }
        Ok(())
    }

    async fn insert_raw_events(&self, batch: &[RawEventRow]) -> anyhow::Result<()> {
        let mut db = self.db.lock().unwrap();
        db.raw_events.extend_from_slice(batch);
        Ok(())
    }

    async fn insert_trades(&self, batch: &[TradeRow]) -> anyhow::Result<()> {
        let mut db = self.db.lock().unwrap();
        db.trades.extend_from_slice(batch);
        Ok(())
    }

    async fn insert_level_changes(&self, batch: &[LevelChangeRow]) -> anyhow::Result<()> {
        let mut db = self.db.lock().unwrap();
        db.level_changes.extend_from_slice(batch);
        Ok(())
    }

    async fn insert_snapshot(&self, snapshot: &SnapshotRow) -> anyhow::Result<()> {
        let mut db = self.db.lock().unwrap();
        db.snapshots.push(snapshot.clone());
        Ok(())
    }

    async fn get_session(&self, session_id: Uuid) -> anyhow::Result<Option<SessionRecord>> {
        Ok(self.db.lock().unwrap().sessions.get(&session_id).cloned())
    }

    async fn list_sessions(&self, limit: u64) -> anyhow::Result<Vec<SessionRecord>> {
        let mut all: Vec<SessionRecord> =
            self.db.lock().unwrap().sessions.values().cloned().collect();
        all.sort_by_key(|b| std::cmp::Reverse(b.started_at));
        all.truncate(limit as usize);
        Ok(all)
    }

    async fn read_snapshots(&self, session_id: Uuid) -> anyhow::Result<Vec<SnapshotRow>> {
        let mut rows: Vec<SnapshotRow> =
            in_session(&self.db.lock().unwrap().snapshots, session_id, |r| {
                r.session_id
            });
        rows.sort_by_key(|r| r.seq);
        Ok(rows)
    }

    async fn read_level_changes(&self, session_id: Uuid) -> anyhow::Result<Vec<LevelChangeRow>> {
        let mut rows: Vec<LevelChangeRow> =
            in_session(&self.db.lock().unwrap().level_changes, session_id, |r| {
                r.session_id
            });
        rows.sort_by_key(|r| r.seq);
        Ok(rows)
    }

    async fn read_level_changes_range(
        &self,
        symbol: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> anyhow::Result<Vec<LevelChangeRow>> {
        let db = self.db.lock().unwrap();
        let mut rows: Vec<LevelChangeRow> = db
            .level_changes
            .iter()
            .filter(|r| {
                r.symbol == symbol
                    && crate::storage::datetime_to_ms(r.exchange_event_time) >= start_ms
                    && crate::storage::datetime_to_ms(r.exchange_event_time) <= end_ms
            })
            .cloned()
            .collect();
        rows.sort_by_key(|r| r.seq);
        Ok(rows)
    }

    async fn read_trades(&self, session_id: Uuid) -> anyhow::Result<Vec<TradeRow>> {
        let mut rows: Vec<TradeRow> =
            in_session(&self.db.lock().unwrap().trades, session_id, |r| {
                r.session_id
            });
        rows.sort_by_key(|r| r.seq);
        Ok(rows)
    }

    async fn read_trades_range(
        &self,
        symbol: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> anyhow::Result<Vec<TradeRow>> {
        let db = self.db.lock().unwrap();
        let mut rows: Vec<TradeRow> = db
            .trades
            .iter()
            .filter(|r| {
                r.symbol == symbol
                    && crate::storage::datetime_to_ms(r.exchange_event_time) >= start_ms
                    && crate::storage::datetime_to_ms(r.exchange_event_time) <= end_ms
            })
            .cloned()
            .collect();
        rows.sort_by_key(|r| r.seq);
        Ok(rows)
    }

    async fn read_raw_events(&self, session_id: Uuid) -> anyhow::Result<Vec<RawEventRow>> {
        let mut rows: Vec<RawEventRow> =
            in_session(&self.db.lock().unwrap().raw_events, session_id, |r| {
                r.session_id
            });
        rows.sort_by_key(|r| r.seq);
        Ok(rows)
    }

    async fn read_raw_events_range(
        &self,
        symbol: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> anyhow::Result<Vec<RawEventRow>> {
        let db = self.db.lock().unwrap();
        let mut rows: Vec<RawEventRow> = db
            .raw_events
            .iter()
            .filter(|r| {
                r.symbol == symbol
                    && crate::storage::datetime_to_ms(r.exchange_event_time) >= start_ms
                    && crate::storage::datetime_to_ms(r.exchange_event_time) <= end_ms
            })
            .cloned()
            .collect();
        rows.sort_by_key(|r| r.seq);
        Ok(rows)
    }

    async fn count_trades(&self, session_id: Uuid) -> anyhow::Result<u64> {
        Ok(self
            .db
            .lock()
            .unwrap()
            .trades
            .iter()
            .filter(|r| r.session_id == session_id)
            .count() as u64)
    }

    async fn count_level_changes(&self, session_id: Uuid) -> anyhow::Result<u64> {
        Ok(self
            .db
            .lock()
            .unwrap()
            .level_changes
            .iter()
            .filter(|r| r.session_id == session_id)
            .count() as u64)
    }

    async fn count_depth_events(&self, session_id: Uuid) -> anyhow::Result<u64> {
        let mut seqs = std::collections::HashSet::new();
        for r in &self.db.lock().unwrap().level_changes {
            if r.session_id == session_id {
                seqs.insert(r.seq);
            }
        }
        Ok(seqs.len() as u64)
    }

    async fn count_raw_events(&self, session_id: Uuid) -> anyhow::Result<u64> {
        Ok(self
            .db
            .lock()
            .unwrap()
            .raw_events
            .iter()
            .filter(|r| r.session_id == session_id)
            .count() as u64)
    }

    async fn database_size(&self) -> anyhow::Result<u64> {
        let db = self.db.lock().unwrap();
        let raw: u64 = db
            .raw_events
            .iter()
            .map(|r| r.raw_payload.len() as u64)
            .sum();
        Ok(raw + db.trades.len() as u64 * 64 + db.level_changes.len() as u64 * 48)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// A storage wrapper that fails inserts for a configurable number of calls.
/// Used to test retry behaviour and failure reporting without a real server.
#[derive(Clone)]
pub struct FlakyStorage {
    inner: Arc<dyn Storage>,
    fail_count: Arc<std::sync::atomic::AtomicUsize>,
}

impl FlakyStorage {
    pub fn new(inner: Arc<dyn Storage>, fail_count: usize) -> Self {
        Self {
            inner,
            fail_count: Arc::new(std::sync::atomic::AtomicUsize::new(fail_count)),
        }
    }

    fn should_fail(&self) -> bool {
        let mut cur = self.fail_count.load(std::sync::atomic::Ordering::SeqCst);
        loop {
            if cur == 0 {
                return false;
            }
            match self.fail_count.compare_exchange(
                cur,
                cur - 1,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            ) {
                Ok(_) => return true,
                Err(actual) => cur = actual,
            }
        }
    }
}

#[async_trait::async_trait]
impl Storage for FlakyStorage {
    async fn ping(&self) -> anyhow::Result<()> {
        self.inner.ping().await
    }

    async fn init_schema(&self) -> anyhow::Result<()> {
        self.inner.init_schema().await
    }

    async fn insert_session(&self, session: &SessionRecord) -> anyhow::Result<()> {
        self.inner.insert_session(session).await
    }

    async fn update_session_status(
        &self,
        session_id: Uuid,
        status: &str,
        ended_at: Option<DateTime<Utc>>,
    ) -> anyhow::Result<()> {
        self.inner
            .update_session_status(session_id, status, ended_at)
            .await
    }

    async fn insert_raw_events(&self, batch: &[RawEventRow]) -> anyhow::Result<()> {
        if self.should_fail() {
            anyhow::bail!("injected storage failure for insert_raw_events()");
        }
        self.inner.insert_raw_events(batch).await
    }

    async fn insert_trades(&self, batch: &[TradeRow]) -> anyhow::Result<()> {
        if self.should_fail() {
            anyhow::bail!("injected storage failure for insert_trades()");
        }
        self.inner.insert_trades(batch).await
    }

    async fn insert_level_changes(&self, batch: &[LevelChangeRow]) -> anyhow::Result<()> {
        if self.should_fail() {
            anyhow::bail!("injected storage failure for insert_level_changes()");
        }
        self.inner.insert_level_changes(batch).await
    }

    async fn insert_snapshot(&self, snapshot: &SnapshotRow) -> anyhow::Result<()> {
        if self.should_fail() {
            anyhow::bail!("injected storage failure for insert_snapshot()");
        }
        self.inner.insert_snapshot(snapshot).await
    }

    async fn get_session(&self, session_id: Uuid) -> anyhow::Result<Option<SessionRecord>> {
        self.inner.get_session(session_id).await
    }

    async fn list_sessions(&self, limit: u64) -> anyhow::Result<Vec<SessionRecord>> {
        self.inner.list_sessions(limit).await
    }

    async fn read_snapshots(&self, session_id: Uuid) -> anyhow::Result<Vec<SnapshotRow>> {
        self.inner.read_snapshots(session_id).await
    }

    async fn read_level_changes(&self, session_id: Uuid) -> anyhow::Result<Vec<LevelChangeRow>> {
        self.inner.read_level_changes(session_id).await
    }

    async fn read_level_changes_range(
        &self,
        symbol: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> anyhow::Result<Vec<LevelChangeRow>> {
        self.inner
            .read_level_changes_range(symbol, start_ms, end_ms)
            .await
    }

    async fn read_trades(&self, session_id: Uuid) -> anyhow::Result<Vec<TradeRow>> {
        self.inner.read_trades(session_id).await
    }

    async fn read_trades_range(
        &self,
        symbol: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> anyhow::Result<Vec<TradeRow>> {
        self.inner.read_trades_range(symbol, start_ms, end_ms).await
    }

    async fn read_raw_events(&self, session_id: Uuid) -> anyhow::Result<Vec<RawEventRow>> {
        self.inner.read_raw_events(session_id).await
    }

    async fn read_raw_events_range(
        &self,
        symbol: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> anyhow::Result<Vec<RawEventRow>> {
        self.inner
            .read_raw_events_range(symbol, start_ms, end_ms)
            .await
    }

    async fn count_trades(&self, session_id: Uuid) -> anyhow::Result<u64> {
        self.inner.count_trades(session_id).await
    }

    async fn count_level_changes(&self, session_id: Uuid) -> anyhow::Result<u64> {
        self.inner.count_level_changes(session_id).await
    }

    async fn count_depth_events(&self, session_id: Uuid) -> anyhow::Result<u64> {
        self.inner.count_depth_events(session_id).await
    }

    async fn count_raw_events(&self, session_id: Uuid) -> anyhow::Result<u64> {
        self.inner.count_raw_events(session_id).await
    }

    async fn database_size(&self) -> anyhow::Result<u64> {
        self.inner.database_size().await
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self.inner.as_any()
    }
}

//! ClickHouse-backed [`Storage`] implementation.
//!
//! Uses the official `clickhouse` crate (HTTP transport, `RowBinaryWithNamesAndTypes`).
//! Inserts are batched by the recording storage worker; this type performs one
//! multi-row `INSERT` per call.

use ::clickhouse::Client;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::migrations;
use super::{
    AnalyticsEventRow, AnalyticsSnapshotRow, DeltaByPriceRow, LevelChangeRow, LiquidityEventRow,
    RawEventRow, SnapshotRow, Storage, TradeRow,
};
use crate::recording::session::SessionRecord;

const SELECT_SESSION: &str = "SELECT session_id, exchange, market_type, symbol, contract_type, \
     started_at, ended_at, software_version, git_commit, depth_stream, trade_stream, status \
     FROM sessions";

/// Escape a database identifier (backticks) to prevent injection.
fn escape_ident(s: &str) -> String {
    s.replace('`', "")
}

pub struct ClickHouseStorage {
    client: Client,
    database: String,
}

impl ClickHouseStorage {
    /// Connect to ClickHouse, creating the database if it does not exist.
    pub async fn connect(
        url: &str,
        database: &str,
        user: &str,
        password: &str,
    ) -> anyhow::Result<Self> {
        let db = escape_ident(database);
        let base = Client::default().with_url(url);
        let base = if user.is_empty() {
            base
        } else {
            base.with_user(user)
        };
        let base = if password.is_empty() {
            base
        } else {
            base.with_password(password)
        };

        // Create database without selecting it first.
        base.query(format!("CREATE DATABASE IF NOT EXISTS `{}`", db).as_str())
            .execute()
            .await?;

        let client = base.with_database(&db);
        Ok(Self {
            client,
            database: db,
        })
    }

    fn client(&self) -> &Client {
        &self.client
    }
}

#[derive(Debug, ::clickhouse::Row, serde::Deserialize)]
struct CountRow {
    n: u64,
}

#[async_trait::async_trait]
impl Storage for ClickHouseStorage {
    async fn ping(&self) -> anyhow::Result<()> {
        self.client.query("SELECT 1").execute().await?;
        Ok(())
    }

    async fn init_schema(&self) -> anyhow::Result<()> {
        migrations::apply_migrations(self.client()).await
    }

    async fn insert_session(&self, session: &SessionRecord) -> anyhow::Result<()> {
        let mut insert = self.client.insert::<SessionRecord>("sessions").await?;
        insert.write(session).await?;
        insert.end().await?;
        Ok(())
    }

    async fn update_session_status(
        &self,
        session_id: Uuid,
        status: &str,
        ended_at: Option<DateTime<Utc>>,
    ) -> anyhow::Result<()> {
        let query = if ended_at.is_some() {
            "ALTER TABLE sessions UPDATE status = ?, ended_at = ? WHERE session_id = ?"
        } else {
            "ALTER TABLE sessions UPDATE status = ? WHERE session_id = ?"
        };
        let mut q = self.client.query(query).with_setting("mutations_sync", "1");
        q = q.bind(status);
        if let Some(end) = ended_at {
            // Bind as a formatted string so ClickHouse parses it as a literal
            // DateTime64; binding raw epoch millis is misread as seconds and
            // overflows the DateTime64 range.
            let formatted = end.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
            q = q.bind(formatted);
        }
        q = q.bind(session_id);
        q.execute().await?;
        Ok(())
    }

    async fn insert_raw_events(&self, batch: &[RawEventRow]) -> anyhow::Result<()> {
        insert_batch(&self.client, "raw_market_events", batch).await
    }

    async fn insert_trades(&self, batch: &[TradeRow]) -> anyhow::Result<()> {
        insert_batch(&self.client, "trades", batch).await
    }

    async fn insert_level_changes(&self, batch: &[LevelChangeRow]) -> anyhow::Result<()> {
        insert_batch(&self.client, "order_book_updates", batch).await
    }

    async fn insert_snapshot(&self, snapshot: &SnapshotRow) -> anyhow::Result<()> {
        let mut insert = self
            .client
            .insert::<SnapshotRow>("order_book_snapshots")
            .await?;
        insert.write(snapshot).await?;
        insert.end().await?;
        Ok(())
    }

    async fn get_session(&self, session_id: Uuid) -> anyhow::Result<Option<SessionRecord>> {
        let mut q = self
            .client
            .query(&format!("{} WHERE session_id = ? LIMIT 1", SELECT_SESSION));
        q = q.bind(session_id);
        let rows: Vec<SessionRecord> = q.fetch_all().await?;
        Ok(rows.into_iter().next())
    }

    async fn list_sessions(&self, limit: u64) -> anyhow::Result<Vec<SessionRecord>> {
        let rows: Vec<SessionRecord> = self
            .client
            .query(&format!(
                "{} ORDER BY started_at DESC LIMIT ?",
                SELECT_SESSION
            ))
            .bind(limit)
            .fetch_all()
            .await?;
        Ok(rows)
    }

    async fn read_snapshots(&self, session_id: Uuid) -> anyhow::Result<Vec<SnapshotRow>> {
        let mut q = self.client.query(
            "SELECT session_id, seq, symbol, snapshot_update_id, timestamp, bids, asks \
             FROM order_book_snapshots WHERE session_id = ? ORDER BY seq",
        );
        q = q.bind(session_id);
        Ok(q.fetch_all().await?)
    }

    async fn read_level_changes(&self, session_id: Uuid) -> anyhow::Result<Vec<LevelChangeRow>> {
        let mut q = self.client.query(
            "SELECT session_id, seq, symbol, exchange_event_time, exchange_transaction_time, \
             local_receive_time, first_update_id, final_update_id, previous_final_update_id, \
             side, price, quantity \
             FROM order_book_updates WHERE session_id = ? ORDER BY seq",
        );
        q = q.bind(session_id);
        Ok(q.fetch_all().await?)
    }

    async fn read_level_changes_range(
        &self,
        symbol: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> anyhow::Result<Vec<LevelChangeRow>> {
        let mut q = self.client.query(
            "SELECT session_id, seq, symbol, exchange_event_time, exchange_transaction_time, \
             local_receive_time, first_update_id, final_update_id, previous_final_update_id, \
             side, price, quantity \
             FROM order_book_updates \
             WHERE symbol = ? AND exchange_event_time >= fromUnixTimestamp64Milli(toInt64(?)) \
             AND exchange_event_time <= fromUnixTimestamp64Milli(toInt64(?)) ORDER BY seq",
        );
        q = q.bind(symbol).bind(start_ms).bind(end_ms);
        Ok(q.fetch_all().await?)
    }

    async fn read_trades(&self, session_id: Uuid) -> anyhow::Result<Vec<TradeRow>> {
        let mut q = self.client.query(
            "SELECT session_id, seq, symbol, trade_id, first_trade_id, last_trade_id, price, \
             quantity, aggressor_side, exchange_event_time, trade_time, local_receive_time, \
             order_type \
             FROM trades WHERE session_id = ? ORDER BY seq",
        );
        q = q.bind(session_id);
        Ok(q.fetch_all().await?)
    }

    async fn read_trades_range(
        &self,
        symbol: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> anyhow::Result<Vec<TradeRow>> {
        let mut q = self.client.query(
            "SELECT session_id, seq, symbol, trade_id, first_trade_id, last_trade_id, price, \
             quantity, aggressor_side, exchange_event_time, trade_time, local_receive_time, \
             order_type \
             FROM trades \
             WHERE symbol = ? AND exchange_event_time >= fromUnixTimestamp64Milli(toInt64(?)) \
             AND exchange_event_time <= fromUnixTimestamp64Milli(toInt64(?)) ORDER BY seq",
        );
        q = q.bind(symbol).bind(start_ms).bind(end_ms);
        Ok(q.fetch_all().await?)
    }

    async fn read_raw_events(&self, session_id: Uuid) -> anyhow::Result<Vec<RawEventRow>> {
        let mut q = self.client.query(
            "SELECT session_id, seq, symbol, stream_type, exchange_event_time, \
             exchange_transaction_time, local_receive_time, raw_payload \
             FROM raw_market_events WHERE session_id = ? ORDER BY seq",
        );
        q = q.bind(session_id);
        Ok(q.fetch_all().await?)
    }

    async fn read_raw_events_range(
        &self,
        symbol: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> anyhow::Result<Vec<RawEventRow>> {
        let mut q = self.client.query(
            "SELECT session_id, seq, symbol, stream_type, exchange_event_time, \
             exchange_transaction_time, local_receive_time, raw_payload \
             FROM raw_market_events \
             WHERE symbol = ? AND exchange_event_time >= fromUnixTimestamp64Milli(toInt64(?)) \
             AND exchange_event_time <= fromUnixTimestamp64Milli(toInt64(?)) ORDER BY seq",
        );
        q = q.bind(symbol).bind(start_ms).bind(end_ms);
        Ok(q.fetch_all().await?)
    }

    async fn count_trades(&self, session_id: Uuid) -> anyhow::Result<u64> {
        count(
            &self.client,
            "SELECT count() AS n FROM trades WHERE session_id = ?",
            session_id,
        )
        .await
    }

    async fn count_level_changes(&self, session_id: Uuid) -> anyhow::Result<u64> {
        count(
            &self.client,
            "SELECT count() AS n FROM order_book_updates WHERE session_id = ?",
            session_id,
        )
        .await
    }

    async fn count_depth_events(&self, session_id: Uuid) -> anyhow::Result<u64> {
        count(
            &self.client,
            "SELECT uniqExact(seq) AS n FROM order_book_updates WHERE session_id = ?",
            session_id,
        )
        .await
    }

    async fn count_raw_events(&self, session_id: Uuid) -> anyhow::Result<u64> {
        count(
            &self.client,
            "SELECT count() AS n FROM raw_market_events WHERE session_id = ?",
            session_id,
        )
        .await
    }

    async fn database_size(&self) -> anyhow::Result<u64> {
        let mut q = self.client.query(
            "SELECT toUInt64(coalesce(sum(bytes_on_disk), 0)) AS n \
             FROM system.parts WHERE database = ? AND active",
        );
        q = q.bind(&self.database);
        let row = q.fetch_one::<CountRow>().await?;
        Ok(row.n)
    }

    async fn insert_analytics_snapshots(
        &self,
        batch: &[AnalyticsSnapshotRow],
    ) -> anyhow::Result<()> {
        insert_batch(&self.client, "analytics_snapshots", batch).await
    }

    async fn insert_analytics_events(&self, batch: &[AnalyticsEventRow]) -> anyhow::Result<()> {
        insert_batch(&self.client, "analytics_events", batch).await
    }

    async fn insert_delta_by_price(&self, batch: &[DeltaByPriceRow]) -> anyhow::Result<()> {
        insert_batch(&self.client, "delta_by_price", batch).await
    }

    async fn insert_liquidity_events(&self, batch: &[LiquidityEventRow]) -> anyhow::Result<()> {
        insert_batch(&self.client, "liquidity_events", batch).await
    }

    async fn read_analytics_snapshots(
        &self,
        session_id: Uuid,
    ) -> anyhow::Result<Vec<AnalyticsSnapshotRow>> {
        let mut q = self.client.query(
            "SELECT session_id, symbol, timestamp_ms, analytics_version, book_ready, best_bid, \
             best_ask, mid_price, spread_ticks, microprice_num, microprice_den, trade_volume, \
             buy_volume, sell_volume, delta, cvd, bid_depth, ask_depth, book_imbalance, \
             liquidity_added, liquidity_removed, large_trade_count, sweep_candidate_count, \
             absorption_candidate_count, replenishment_count, book_crossed, anomalies \
             FROM analytics_snapshots WHERE session_id = ? ORDER BY timestamp_ms",
        );
        q = q.bind(session_id);
        Ok(q.fetch_all().await?)
    }

    async fn read_analytics_events(
        &self,
        session_id: Uuid,
    ) -> anyhow::Result<Vec<AnalyticsEventRow>> {
        let mut q = self.client.query(
            "SELECT session_id, symbol, ts_ms, kind, side, price, quantity, detail \
             FROM analytics_events WHERE session_id = ? ORDER BY ts_ms",
        );
        q = q.bind(session_id);
        Ok(q.fetch_all().await?)
    }

    async fn read_delta_by_price(&self, session_id: Uuid) -> anyhow::Result<Vec<DeltaByPriceRow>> {
        let mut q = self.client.query(
            "SELECT session_id, symbol, ts_ms, price, buy_volume, sell_volume, total_volume, \
             delta, trade_count, large_trade_count \
             FROM delta_by_price WHERE session_id = ? ORDER BY price",
        );
        q = q.bind(session_id);
        Ok(q.fetch_all().await?)
    }

    async fn read_liquidity_events(
        &self,
        session_id: Uuid,
    ) -> anyhow::Result<Vec<LiquidityEventRow>> {
        let mut q = self.client.query(
            "SELECT session_id, symbol, ts_ms, kind, side, price, quantity_delta, is_replenishment \
             FROM liquidity_events WHERE session_id = ? ORDER BY ts_ms",
        );
        q = q.bind(session_id);
        Ok(q.fetch_all().await?)
    }

    async fn count_analytics_snapshots(&self, session_id: Uuid) -> anyhow::Result<u64> {
        count(
            &self.client,
            "SELECT count() AS n FROM analytics_snapshots WHERE session_id = ?",
            session_id,
        )
        .await
    }

    async fn count_analytics_events(&self, session_id: Uuid) -> anyhow::Result<u64> {
        count(
            &self.client,
            "SELECT count() AS n FROM analytics_events WHERE session_id = ?",
            session_id,
        )
        .await
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Insert a batch of rows into a table using the streaming insert API.
async fn insert_batch<T>(client: &Client, table: &str, batch: &[T]) -> anyhow::Result<()>
where
    T: ::clickhouse::RowOwned + ::clickhouse::RowWrite + serde::Serialize + Send + Sync,
{
    if batch.is_empty() {
        return Ok(());
    }
    let mut insert = client.insert::<T>(table).await?;
    for row in batch {
        insert.write(row).await?;
    }
    insert.end().await?;
    Ok(())
}

async fn count(client: &Client, sql: &str, session_id: Uuid) -> anyhow::Result<u64> {
    let mut q = client.query(sql);
    q = q.bind(session_id);
    let row = q.fetch_one::<CountRow>().await?;
    Ok(row.n)
}

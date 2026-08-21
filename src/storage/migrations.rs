//! Simple versioned schema migrations for ClickHouse.
//!
//! SQL files live in `migrations/` and are embedded at compile time. A
//! `schema_migrations` table tracks applied versions; pending migrations are
//! applied in filename order and then recorded. Running again is a no-op.

use ::clickhouse::Client;

/// Ordered list of (version_name, embedded SQL).
pub const MIGRATIONS: &[(&str, &str)] = &[
    (
        "001_create_sessions",
        include_str!("../../migrations/001_create_sessions.sql"),
    ),
    (
        "002_create_raw_market_events",
        include_str!("../../migrations/002_create_raw_market_events.sql"),
    ),
    (
        "003_create_trades",
        include_str!("../../migrations/003_create_trades.sql"),
    ),
    (
        "004_create_order_book_updates",
        include_str!("../../migrations/004_create_order_book_updates.sql"),
    ),
    (
        "005_create_order_book_snapshots",
        include_str!("../../migrations/005_create_order_book_snapshots.sql"),
    ),
    (
        "006_create_analytics_snapshots",
        include_str!("../../migrations/006_create_analytics_snapshots.sql"),
    ),
    (
        "007_create_analytics_events",
        include_str!("../../migrations/007_create_analytics_events.sql"),
    ),
    (
        "008_create_delta_by_price",
        include_str!("../../migrations/008_create_delta_by_price.sql"),
    ),
    (
        "009_create_liquidity_events",
        include_str!("../../migrations/009_create_liquidity_events.sql"),
    ),
];

/// Apply all pending migrations. Idempotent.
pub async fn apply_migrations(client: &Client) -> anyhow::Result<()> {
    client
        .query(
            "CREATE TABLE IF NOT EXISTS schema_migrations (\
             version String,\
             applied_at DateTime DEFAULT now()\
             ) ENGINE = MergeTree ORDER BY version",
        )
        .execute()
        .await?;

    let applied: Vec<String> = client
        .query("SELECT version FROM schema_migrations")
        .fetch_all::<MigrationRow>()
        .await?
        .into_iter()
        .map(|r| r.version)
        .collect();

    for (name, sql) in MIGRATIONS {
        if applied.iter().any(|v| v == name) {
            continue;
        }
        client.query(sql).execute().await?;
        client
            .query("INSERT INTO schema_migrations (version) VALUES (?)")
            .bind(name)
            .execute()
            .await?;
    }
    Ok(())
}

#[derive(Debug, ::clickhouse::Row, serde::Deserialize)]
struct MigrationRow {
    version: String,
}

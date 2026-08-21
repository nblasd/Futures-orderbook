CREATE TABLE IF NOT EXISTS analytics_events (
    session_id UUID,
    symbol String,
    ts_ms DateTime64(3, 'UTC'),
    kind String,
    side Nullable(String),
    price Nullable(UInt64),
    quantity UInt64,
    detail String
)
ENGINE = MergeTree
ORDER BY (session_id, ts_ms, kind)
CREATE TABLE IF NOT EXISTS liquidity_events (
    session_id UUID,
    symbol String,
    ts_ms DateTime64(3, 'UTC'),
    kind String,
    side String,
    price UInt64,
    quantity_delta UInt64,
    is_replenishment UInt8
)
ENGINE = MergeTree
ORDER BY (session_id, ts_ms)
CREATE TABLE IF NOT EXISTS delta_by_price (
    session_id UUID,
    symbol String,
    ts_ms DateTime64(3, 'UTC'),
    price UInt64,
    buy_volume UInt64,
    sell_volume UInt64,
    total_volume UInt64,
    delta Int128,
    trade_count UInt64,
    large_trade_count UInt64
)
ENGINE = MergeTree
ORDER BY (session_id, price)
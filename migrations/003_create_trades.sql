CREATE TABLE IF NOT EXISTS trades (
    session_id UUID,
    seq UInt64,
    symbol String,
    trade_id UInt64,
    first_trade_id Nullable(UInt64),
    last_trade_id Nullable(UInt64),
    price UInt64,
    quantity UInt64,
    aggressor_side String,
    exchange_event_time DateTime64(3, 'UTC'),
    trade_time DateTime64(3, 'UTC'),
    local_receive_time DateTime64(9, 'UTC'),
    order_type String
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(exchange_event_time)
ORDER BY (session_id, symbol, exchange_event_time, trade_id, seq)
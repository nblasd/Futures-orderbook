CREATE TABLE IF NOT EXISTS raw_market_events (
    session_id UUID,
    seq UInt64,
    symbol String,
    stream_type String,
    exchange_event_time DateTime64(3, 'UTC'),
    exchange_transaction_time Nullable(DateTime64(3, 'UTC')),
    local_receive_time DateTime64(9, 'UTC'),
    raw_payload String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(local_receive_time)
ORDER BY (session_id, symbol, exchange_event_time, seq)
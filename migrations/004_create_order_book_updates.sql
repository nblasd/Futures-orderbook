CREATE TABLE IF NOT EXISTS order_book_updates (
    session_id UUID,
    seq UInt64,
    symbol String,
    exchange_event_time DateTime64(3, 'UTC'),
    exchange_transaction_time DateTime64(3, 'UTC'),
    local_receive_time DateTime64(9, 'UTC'),
    first_update_id UInt64,
    final_update_id UInt64,
    previous_final_update_id UInt64,
    side String,
    price UInt64,
    quantity UInt64
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(exchange_event_time)
ORDER BY (session_id, symbol, exchange_event_time, seq, final_update_id, side, price)
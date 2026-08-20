CREATE TABLE IF NOT EXISTS order_book_snapshots (
    session_id UUID,
    seq UInt64,
    symbol String,
    snapshot_update_id UInt64,
    timestamp DateTime64(3, 'UTC'),
    bids String,
    asks String
)
ENGINE = MergeTree
ORDER BY (session_id, symbol, timestamp, snapshot_update_id)
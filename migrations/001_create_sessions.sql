CREATE TABLE IF NOT EXISTS sessions (
    session_id UUID,
    exchange String,
    market_type String,
    symbol String,
    contract_type String,
    started_at DateTime64(3, 'UTC'),
    ended_at Nullable(DateTime64(3, 'UTC')),
    software_version String,
    git_commit String,
    depth_stream String,
    trade_stream String,
    status String
)
ENGINE = MergeTree
ORDER BY (session_id)
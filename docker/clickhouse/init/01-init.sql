CREATE DATABASE IF NOT EXISTS test;

CREATE TABLE IF NOT EXISTS test.events (
    id UInt64,
    event String,
    created_at DateTime
)
ENGINE = MergeTree
ORDER BY (id);

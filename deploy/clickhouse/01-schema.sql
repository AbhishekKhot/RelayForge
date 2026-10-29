CREATE DATABASE IF NOT EXISTS relayforge_analytics;

CREATE TABLE IF NOT EXISTS relayforge_analytics.delivery_facts
(
    organization_id UUID,
    event_date Date,
    fact_id UUID,
    version UInt64,
    event_id Nullable(UUID),
    delivery_id Nullable(UUID),
    endpoint_id Nullable(UUID),
    replay_generation Nullable(UInt32),
    fact_type LowCardinality(String),
    status LowCardinality(String),
    attempt_number Nullable(UInt16),
    http_status Nullable(UInt16),
    duration_ms Nullable(UInt32),
    error_code LowCardinality(String) DEFAULT '',
    occurred_at DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(event_date)
ORDER BY (organization_id, event_date, fact_id);

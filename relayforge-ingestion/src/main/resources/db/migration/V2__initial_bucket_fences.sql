INSERT INTO bucket_fence (bucket_id, epoch, state)
SELECT bucket_id, 1, 'ACTIVE'
FROM generate_series(0, 4095) AS bucket_id;

CREATE TABLE ingestion_rate_counters (
    organization_id uuid NOT NULL,
    minute_start timestamptz NOT NULL,
    accepted_count integer NOT NULL CHECK (accepted_count > 0),
    PRIMARY KEY (organization_id, minute_start)
);
CREATE INDEX ingestion_rate_counters_expiry_idx ON ingestion_rate_counters (minute_start);

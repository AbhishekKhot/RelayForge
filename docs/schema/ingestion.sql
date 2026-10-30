SET TIME ZONE 'UTC';

CREATE TABLE bucket_fence (
    bucket_id smallint PRIMARY KEY CHECK (bucket_id BETWEEN 0 AND 4095),
    epoch bigint NOT NULL CHECK (epoch > 0),
    state varchar(20) NOT NULL CHECK (state IN ('ACTIVE', 'DRAINING'))
);

CREATE TABLE idempotency_registry (
    organization_id uuid NOT NULL,
    idempotency_key varchar(255) NOT NULL,
    bucket_id smallint NOT NULL CHECK (bucket_id BETWEEN 0 AND 4095),
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    request_hash_version smallint NOT NULL CHECK (request_hash_version > 0),
    event_id uuid NOT NULL,
    event_locator text NOT NULL,
    accepted_day date NOT NULL,
    accepted_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (organization_id, idempotency_key),
    CHECK (expires_at = accepted_at + interval '2160 hours'),
    CHECK (accepted_day = (accepted_at AT TIME ZONE 'UTC')::date)
);
CREATE INDEX idempotency_registry_expiry_idx ON idempotency_registry (expires_at);
CREATE INDEX idempotency_registry_bucket_idx ON idempotency_registry (bucket_id, expires_at);

CREATE TABLE events (
    accepted_day date NOT NULL,
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    organization_id uuid NOT NULL,
    event_type varchar(200) NOT NULL,
    occurred_at timestamptz,
    schema_version integer NOT NULL DEFAULT 1,
    payload jsonb NOT NULL,
    payload_hash bytea NOT NULL CHECK (octet_length(payload_hash) = 32),
    idempotency_key varchar(255) NOT NULL,
    accepted_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (accepted_day, id),
    CHECK (accepted_day = (accepted_at AT TIME ZONE 'UTC')::date)
) PARTITION BY RANGE (accepted_day);
CREATE INDEX events_org_accepted_idx ON events (organization_id, accepted_at DESC, id DESC);
CREATE INDEX events_route_idx ON events (accepted_day, organization_id, event_type);

CREATE TABLE ingestion_outbox (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id uuid NOT NULL,
    accepted_day date NOT NULL,
    event_id uuid NOT NULL,
    message_type varchar(100) NOT NULL,
    topic varchar(200) NOT NULL,
    partition_key varchar(200) NOT NULL,
    payload jsonb NOT NULL,
    status varchar(20) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'IN_FLIGHT', 'PUBLISHED')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    claimed_by varchar(160),
    claim_token uuid,
    claim_expires_at timestamptz,
    published_at timestamptz,
    publish_attempts integer NOT NULL DEFAULT 0,
    last_error_code varchar(80),
    UNIQUE (event_id, message_type)
);
CREATE INDEX ingestion_outbox_pending_idx ON ingestion_outbox (available_at, created_at) WHERE status IN ('PENDING', 'IN_FLIGHT');
CREATE INDEX ingestion_outbox_cohort_idx ON ingestion_outbox (accepted_day, status);

DO $$
DECLARE
    day_offset integer;
    day_value date;
BEGIN
    FOR day_offset IN -1..7 LOOP
        day_value := current_date + day_offset;
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS events_%s PARTITION OF events FOR VALUES FROM (%L) TO (%L)',
            to_char(day_value, 'YYYYMMDD'), day_value, day_value + 1
        );
    END LOOP;
END
$$;

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

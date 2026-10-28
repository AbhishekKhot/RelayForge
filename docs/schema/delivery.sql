SET TIME ZONE 'UTC';

CREATE TABLE endpoint_projection (
    organization_id uuid NOT NULL,
    endpoint_id uuid NOT NULL,
    status varchar(24) NOT NULL CHECK (status IN ('ACTIVE', 'PAUSED', 'DISABLED')),
    source_version bigint NOT NULL CHECK (source_version > 0),
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (organization_id, endpoint_id)
);

CREATE TABLE subscription_projection (
    organization_id uuid NOT NULL,
    subscription_id uuid NOT NULL,
    endpoint_id uuid NOT NULL,
    event_type varchar(200) NOT NULL,
    filter_config jsonb,
    status varchar(24) NOT NULL CHECK (status IN ('ACTIVE', 'PAUSED', 'DELETED')),
    source_version bigint NOT NULL CHECK (source_version > 0),
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (organization_id, subscription_id),
    FOREIGN KEY (organization_id, endpoint_id) REFERENCES endpoint_projection(organization_id, endpoint_id)
);
CREATE INDEX subscription_projection_route_idx ON subscription_projection (organization_id, event_type, status, endpoint_id);

CREATE TABLE projection_state (
    organization_id uuid PRIMARY KEY,
    source_revision bigint NOT NULL CHECK (source_revision >= 0),
    ready boolean NOT NULL DEFAULT false,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE routing_progress (
    accepted_day date NOT NULL,
    event_id uuid NOT NULL,
    organization_id uuid NOT NULL,
    event_accepted_at timestamptz NOT NULL,
    ingestion_shard_id varchar(80) NOT NULL,
    delivery_shard_id varchar(80) NOT NULL,
    status varchar(24) NOT NULL CHECK (status IN ('SELECTING', 'ROUTING', 'ROUTED', 'NO_SUBSCRIBERS')),
    last_endpoint_id uuid,
    target_count integer NOT NULL DEFAULT 0 CHECK (target_count >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (accepted_day, event_id, organization_id),
    CHECK (accepted_day = (event_accepted_at AT TIME ZONE 'UTC')::date)
) PARTITION BY RANGE (accepted_day);
CREATE INDEX routing_progress_org_idx ON routing_progress (organization_id, accepted_day, event_id);
CREATE INDEX routing_progress_incomplete_idx ON routing_progress (accepted_day, status) WHERE status IN ('SELECTING', 'ROUTING');

CREATE TABLE routing_targets (
    accepted_day date NOT NULL,
    event_id uuid NOT NULL,
    organization_id uuid NOT NULL,
    endpoint_id uuid NOT NULL,
    subscription_id uuid NOT NULL,
    source_version bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (accepted_day, event_id, organization_id, endpoint_id),
    FOREIGN KEY (accepted_day, event_id, organization_id) REFERENCES routing_progress(accepted_day, event_id, organization_id)
) PARTITION BY RANGE (accepted_day);

CREATE TABLE deliveries (
    accepted_day date NOT NULL,
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    organization_id uuid NOT NULL,
    event_id uuid NOT NULL,
    endpoint_id uuid NOT NULL,
    subscription_id uuid,
    generation integer NOT NULL DEFAULT 1 CHECK (generation > 0),
    status varchar(32) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'ENQUEUED', 'IN_PROGRESS', 'RETRY_SCHEDULED', 'SUCCEEDED', 'DEAD_LETTERED', 'CANCELLED')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    claim_count integer NOT NULL DEFAULT 0 CHECK (claim_count >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    lease_owner varchar(160),
    lease_token uuid,
    lease_expires_at timestamptz,
    last_http_status integer,
    last_error_code varchar(80),
    last_error_message varchar(500),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    version bigint NOT NULL DEFAULT 1,
    PRIMARY KEY (accepted_day, id),
    UNIQUE (accepted_day, event_id, endpoint_id, generation),
    FOREIGN KEY (accepted_day, event_id, organization_id) REFERENCES routing_progress(accepted_day, event_id, organization_id)
) PARTITION BY RANGE (accepted_day);
CREATE INDEX deliveries_org_created_idx ON deliveries (organization_id, accepted_day DESC, created_at DESC, id DESC);
CREATE INDEX deliveries_event_idx ON deliveries (accepted_day, organization_id, event_id);
CREATE INDEX deliveries_endpoint_idx ON deliveries (organization_id, endpoint_id, status, created_at DESC);

CREATE TABLE delivery_work (
    delivery_id uuid PRIMARY KEY,
    accepted_day date NOT NULL,
    organization_id uuid NOT NULL,
    endpoint_id uuid NOT NULL,
    status varchar(32) NOT NULL CHECK (status IN ('PENDING', 'ENQUEUED', 'IN_PROGRESS', 'RETRY_SCHEDULED')),
    event_expires_at timestamptz NOT NULL,
    next_attempt_at timestamptz NOT NULL,
    lease_owner varchar(160),
    lease_token uuid,
    lease_expires_at timestamptz,
    claim_count integer NOT NULL DEFAULT 0 CHECK (claim_count >= 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (accepted_day, delivery_id) REFERENCES deliveries(accepted_day, id)
);
CREATE INDEX delivery_work_due_idx ON delivery_work (status, next_attempt_at, delivery_id) WHERE status IN ('PENDING', 'ENQUEUED', 'RETRY_SCHEDULED');
CREATE INDEX delivery_work_lease_idx ON delivery_work (lease_expires_at) WHERE status = 'IN_PROGRESS';
CREATE INDEX delivery_work_cohort_idx ON delivery_work (accepted_day);

CREATE TABLE delivery_attempts (
    accepted_day date NOT NULL,
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    delivery_id uuid NOT NULL,
    attempt_number integer NOT NULL CHECK (attempt_number > 0),
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    outcome varchar(32) NOT NULL,
    http_status integer,
    duration_ms integer,
    error_code varchar(80),
    error_detail varchar(1000),
    response_excerpt text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (accepted_day, id),
    UNIQUE (accepted_day, delivery_id, attempt_number),
    FOREIGN KEY (accepted_day, delivery_id) REFERENCES deliveries(accepted_day, id)
) PARTITION BY RANGE (accepted_day);

CREATE TABLE authentication_attempts (
    accepted_day date NOT NULL,
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    endpoint_id uuid NOT NULL,
    delivery_id uuid NOT NULL,
    attempt_number integer NOT NULL CHECK (attempt_number > 0),
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    outcome varchar(32) NOT NULL,
    http_status integer,
    duration_ms integer,
    error_code varchar(80),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (accepted_day, id),
    UNIQUE (accepted_day, delivery_id, attempt_number),
    FOREIGN KEY (accepted_day, delivery_id) REFERENCES deliveries(accepted_day, id)
) PARTITION BY RANGE (accepted_day);
CREATE INDEX authentication_attempts_delivery_idx ON authentication_attempts (accepted_day, delivery_id, started_at DESC);

CREATE TABLE replay_requests (
    accepted_day date NOT NULL,
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    organization_id uuid NOT NULL,
    original_delivery_id uuid NOT NULL,
    replay_delivery_id uuid NOT NULL,
    replay_key varchar(255) NOT NULL,
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    requested_by_key_id uuid,
    reason varchar(500),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (accepted_day, id),
    UNIQUE (accepted_day, replay_delivery_id),
    UNIQUE (accepted_day, original_delivery_id, replay_key),
    FOREIGN KEY (accepted_day, original_delivery_id) REFERENCES deliveries(accepted_day, id),
    FOREIGN KEY (accepted_day, replay_delivery_id) REFERENCES deliveries(accepted_day, id)
) PARTITION BY RANGE (accepted_day);

CREATE TABLE delivery_audit_records (
    accepted_day date NOT NULL,
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    organization_id uuid NOT NULL,
    delivery_id uuid NOT NULL,
    actor_key_id uuid,
    action varchar(100) NOT NULL,
    request_id varchar(100),
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (accepted_day, id),
    FOREIGN KEY (accepted_day, delivery_id) REFERENCES deliveries(accepted_day, id)
) PARTITION BY RANGE (accepted_day);

CREATE TABLE delivery_outbox (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id uuid NOT NULL,
    accepted_day date NOT NULL,
    event_id uuid NOT NULL,
    delivery_id uuid,
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
    last_error_code varchar(80)
);
CREATE INDEX delivery_outbox_pending_idx ON delivery_outbox (available_at, created_at) WHERE status IN ('PENDING', 'IN_FLIGHT');
CREATE INDEX delivery_outbox_cohort_idx ON delivery_outbox (accepted_day, status);

DO $$
DECLARE
    day_offset integer;
    day_value date;
    table_name text;
BEGIN
    FOR day_offset IN -1..7 LOOP
        day_value := current_date + day_offset;
        FOREACH table_name IN ARRAY ARRAY['routing_progress', 'routing_targets', 'deliveries', 'delivery_attempts', 'authentication_attempts', 'replay_requests', 'delivery_audit_records'] LOOP
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                table_name || '_' || to_char(day_value, 'YYYYMMDD'), table_name, day_value, day_value + 1
            );
        END LOOP;
    END LOOP;
END
$$;

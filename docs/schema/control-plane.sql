CREATE TABLE organizations (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name varchar(160) NOT NULL,
    status varchar(24) NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'SUSPENDED', 'DELETED')),
    plan_code varchar(40) NOT NULL DEFAULT 'FREE',
    quota_config jsonb NOT NULL DEFAULT '{}'::jsonb,
    config_revision bigint NOT NULL DEFAULT 0 CHECK (config_revision >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE api_keys (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id uuid NOT NULL REFERENCES organizations(id),
    key_id varchar(32) NOT NULL UNIQUE,
    secret_hash bytea NOT NULL,
    scopes text[] NOT NULL DEFAULT ARRAY['events:write'],
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_used_at timestamptz,
    expires_at timestamptz,
    revoked_at timestamptz,
    UNIQUE (id, organization_id)
);
CREATE INDEX api_keys_org_idx ON api_keys (organization_id);

CREATE TABLE users (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id uuid NOT NULL REFERENCES organizations(id),
    email varchar(320) NOT NULL,
    display_name varchar(160) NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    status varchar(24) NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'DISABLED')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, email)
);

CREATE TABLE endpoints (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id uuid NOT NULL REFERENCES organizations(id),
    name varchar(160) NOT NULL,
    base_url text NOT NULL,
    path text NOT NULL,
    query_params jsonb NOT NULL DEFAULT '{}'::jsonb,
    headers jsonb NOT NULL DEFAULT '{}'::jsonb,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    auth_type varchar(40) NOT NULL,
    auth_config_ciphertext bytea,
    auth_wrapped_dek bytea,
    auth_key_version varchar(80),
    auth_nonce bytea,
    signing_secret_ciphertext bytea,
    signing_wrapped_dek bytea,
    signing_key_version varchar(80),
    signing_nonce bytea,
    status varchar(24) NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'PAUSED', 'DISABLED')),
    timeout_ms integer NOT NULL DEFAULT 10000 CHECK (timeout_ms BETWEEN 100 AND 60000),
    max_concurrency integer NOT NULL DEFAULT 10 CHECK (max_concurrency BETWEEN 1 AND 1000),
    max_attempts smallint NOT NULL DEFAULT 10 CHECK (max_attempts BETWEEN 1 AND 20),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    version bigint NOT NULL DEFAULT 1,
    UNIQUE (id, organization_id)
);
CREATE INDEX endpoints_org_status_idx ON endpoints (organization_id, status);

CREATE TABLE encryption_key_checks (
    key_version varchar(80) PRIMARY KEY,
    wrapped_check bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE subscriptions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id uuid NOT NULL,
    endpoint_id uuid NOT NULL,
    event_type varchar(200) NOT NULL,
    filter_config jsonb,
    status varchar(24) NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'PAUSED', 'DELETED')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    version bigint NOT NULL DEFAULT 1,
    UNIQUE (id, organization_id),
    UNIQUE (organization_id, endpoint_id, event_type),
    FOREIGN KEY (endpoint_id, organization_id) REFERENCES endpoints(id, organization_id)
);
CREATE INDEX subscriptions_route_idx ON subscriptions (organization_id, event_type, status, endpoint_id);

CREATE TABLE shard_bucket_map (
    bucket_id smallint PRIMARY KEY CHECK (bucket_id BETWEEN 0 AND 4095),
    epoch bigint NOT NULL CHECK (epoch > 0),
    ingestion_shard_id varchar(80) NOT NULL,
    delivery_shard_id varchar(80) NOT NULL,
    state varchar(20) NOT NULL CHECK (state IN ('ACTIVE', 'DRAINING')),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE audit_records (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id uuid REFERENCES organizations(id),
    actor_key_id uuid,
    action varchar(100) NOT NULL,
    resource_type varchar(60) NOT NULL,
    resource_id uuid,
    request_id varchar(100),
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (actor_key_id, organization_id) REFERENCES api_keys(id, organization_id)
);
CREATE INDEX audit_records_org_created_idx ON audit_records (organization_id, created_at DESC);

CREATE TABLE control_outbox (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id uuid,
    source_revision bigint,
    aggregate_type varchar(60) NOT NULL,
    aggregate_id varchar(200) NOT NULL,
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
    UNIQUE (organization_id, source_revision),
    FOREIGN KEY (organization_id) REFERENCES organizations(id)
);
CREATE INDEX control_outbox_pending_idx ON control_outbox (available_at, created_at) WHERE status IN ('PENDING', 'IN_FLIGHT');

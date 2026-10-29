CREATE TABLE provisioning_requests (
    request_hash bytea PRIMARY KEY,
    organization_id uuid NOT NULL REFERENCES organizations(id),
    key_id varchar(32) NOT NULL REFERENCES api_keys(key_id),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX provisioning_requests_organization_idx ON provisioning_requests (organization_id);

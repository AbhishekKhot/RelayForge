CREATE TABLE encryption_key_checks (
    key_version varchar(80) PRIMARY KEY,
    wrapped_check bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

ALTER TABLE endpoints ADD COLUMN auth_credential_version bigint NOT NULL DEFAULT 1
    CHECK (auth_credential_version > 0);

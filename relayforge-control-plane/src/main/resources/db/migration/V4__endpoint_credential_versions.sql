ALTER TABLE endpoints ADD COLUMN auth_config_version bigint CHECK (auth_config_version > 0);
ALTER TABLE endpoints ADD COLUMN signing_secret_version bigint CHECK (signing_secret_version > 0);

UPDATE endpoints SET auth_config_version = 1 WHERE auth_config_ciphertext IS NOT NULL;

CREATE INDEX endpoints_org_id_idx ON endpoints (organization_id, id);
CREATE INDEX subscriptions_org_id_idx ON subscriptions (organization_id, id);

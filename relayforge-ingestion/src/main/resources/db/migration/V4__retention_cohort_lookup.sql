CREATE INDEX idempotency_registry_cohort_expiry_idx
    ON idempotency_registry (accepted_day, expires_at);

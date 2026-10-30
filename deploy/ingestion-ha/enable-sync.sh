#!/usr/bin/env bash
set -euo pipefail
export PGPASSWORD="$POSTGRES_PASSWORD"
psql -h ingestion-primary -U "$POSTGRES_USER" -d postgres -v ON_ERROR_STOP=1 \
    -c "ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (ingestion_standby)'" >/dev/null
psql -h ingestion-primary -U "$POSTGRES_USER" -d postgres -v ON_ERROR_STOP=1 \
    -c 'SELECT pg_reload_conf()' >/dev/null
for ((attempt = 0; attempt < 60; attempt++)); do
    count="$(psql -h ingestion-primary -U "$POSTGRES_USER" -d postgres -At \
        -c "SELECT count(*) FROM pg_stat_replication WHERE application_name = 'ingestion_standby' AND sync_state = 'sync'")"
    if [[ "$count" == 1 ]]; then
        exit 0
    fi
    sleep 1
done
echo 'Synchronous ingestion standby did not become ready' >&2
exit 1

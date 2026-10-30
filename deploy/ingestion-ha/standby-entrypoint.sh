#!/usr/bin/env bash
set -euo pipefail

mkdir -p "$PGDATA"
chown -R postgres:postgres "$(dirname "$PGDATA")"
if [[ ! -s "$PGDATA/PG_VERSION" ]]; then
    for ((attempt = 0; attempt < 60; attempt++)); do
        if pg_isready -h ingestion-primary -U "$POSTGRES_USER" >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    export PGPASSWORD="$INGESTION_REPLICATION_PASSWORD"
    gosu postgres pg_basebackup -h ingestion-primary -U "$INGESTION_REPLICATION_USER" \
        -D "$PGDATA" -Fp -Xs
    unset PGPASSWORD
    touch "$PGDATA/standby.signal"
    printf 'ingestion-primary:5432:*:%s:%s\n' "$INGESTION_REPLICATION_USER" \
        "$INGESTION_REPLICATION_PASSWORD" > "$PGDATA/.pgpass"
    chown postgres:postgres "$PGDATA/.pgpass" "$PGDATA/standby.signal"
    chmod 0600 "$PGDATA/.pgpass"
    printf "primary_conninfo = 'host=ingestion-primary port=5432 user=%s passfile=%s/.pgpass application_name=ingestion_standby'\n" \
        "$INGESTION_REPLICATION_USER" "$PGDATA" >> "$PGDATA/postgresql.auto.conf"
fi
exec docker-entrypoint.sh postgres

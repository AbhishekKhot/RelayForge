#!/usr/bin/env bash
set -euo pipefail

psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --set ON_ERROR_STOP=1 \
    --set ingestion_user="$INGESTION_DB_USER" --set ingestion_password="$INGESTION_DB_PASSWORD" \
    --set replication_user="$INGESTION_REPLICATION_USER" \
    --set replication_password="$INGESTION_REPLICATION_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'ingestion_user', :'ingestion_password') \gexec
SELECT format('CREATE ROLE %I REPLICATION LOGIN PASSWORD %L', :'replication_user', :'replication_password') \gexec
SELECT format('CREATE DATABASE relayforge_ingestion OWNER %I', :'ingestion_user') \gexec
SELECT format('GRANT pg_read_all_stats TO %I', :'ingestion_user') \gexec
SQL
printf 'host replication %s all scram-sha-256\n' "$INGESTION_REPLICATION_USER" >> "$PGDATA/pg_hba.conf"

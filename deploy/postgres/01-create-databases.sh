#!/usr/bin/env bash
set -euo pipefail

psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --set ON_ERROR_STOP=1 \
    --set control_user="$CONTROL_DB_USER" --set control_password="$CONTROL_DB_PASSWORD" \
    --set ingestion_user="$INGESTION_DB_USER" --set ingestion_password="$INGESTION_DB_PASSWORD" \
    --set delivery_user="$DELIVERY_DB_USER" --set delivery_password="$DELIVERY_DB_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'control_user', :'control_password') \gexec
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'ingestion_user', :'ingestion_password') \gexec
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'delivery_user', :'delivery_password') \gexec
SELECT format('CREATE DATABASE relayforge_control OWNER %I', :'control_user') \gexec
SELECT format('CREATE DATABASE relayforge_ingestion OWNER %I', :'ingestion_user') \gexec
SELECT format('CREATE DATABASE relayforge_delivery OWNER %I', :'delivery_user') \gexec
SQL

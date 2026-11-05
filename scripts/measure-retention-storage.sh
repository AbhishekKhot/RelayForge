#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

for database in relayforge_ingestion relayforge_delivery; do
    echo "Storage snapshot for $database"
    docker compose exec -T postgres sh -c \
        'psql -U "$POSTGRES_USER" -d "$1" -v ON_ERROR_STOP=1 -P pager=off' \
        sh "$database" < scripts/retention-storage.sql
done

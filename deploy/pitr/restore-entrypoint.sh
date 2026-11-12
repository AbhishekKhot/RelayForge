#!/usr/bin/env bash
set -euo pipefail

: "${PGDATA:?}"
if [[ ! -s "$PGDATA/PG_VERSION" ]]; then
    mkdir -p "$PGDATA"
    cp -a /backup/. "$PGDATA/"
    chown -R postgres:postgres "$PGDATA"
    find "$PGDATA/pg_wal" -maxdepth 1 -type f -delete
    rm -f "$PGDATA/standby.signal"
    touch "$PGDATA/recovery.signal"
    cat >> "$PGDATA/postgresql.auto.conf" <<'CONF'
restore_command = 'cp /archive/%f %p'
recovery_target_name = 'relayforge_pitr_target'
recovery_target_action = 'promote'
CONF
    chown postgres:postgres "$PGDATA/recovery.signal" "$PGDATA/postgresql.auto.conf"
fi
exec docker-entrypoint.sh "$@"

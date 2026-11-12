#!/usr/bin/env bash
set -euo pipefail

: "${PGDATA:?}"
if [[ -e "$PGDATA/.relayforge-fenced" ]]; then
    echo "Refusing to start fenced old primary" >&2
    exit 1
fi
exec docker-entrypoint.sh "$@"

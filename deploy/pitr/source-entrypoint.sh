#!/usr/bin/env bash
set -euo pipefail

mkdir -p /backup /archive
chown postgres:postgres /backup /archive
chmod 0700 /backup /archive
exec docker-entrypoint.sh "$@"

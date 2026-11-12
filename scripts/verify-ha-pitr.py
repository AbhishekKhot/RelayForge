"""Restore service-owned PostgreSQL state from a physical backup and archived WAL."""

import subprocess
import time
from pathlib import Path
from uuid import uuid4

from verify_support import settings


PROJECT = "relayforge-pitr-" + uuid4().hex[:8]
COMPOSE = ["docker", "compose", "-p", PROJECT, "-f", "compose.yaml",
           "-f", "deploy/pitr/compose.yaml"]
MIGRATIONS = {
    "relayforge_control": Path("relayforge-control-plane/src/main/resources/db/migration"),
    "relayforge_ingestion": Path("relayforge-ingestion/src/main/resources/db/migration"),
    "relayforge_delivery": Path("relayforge-delivery-api/src/main/resources/db/migration"),
}
TABLES = {
    "relayforge_control": {"organizations": 1, "endpoints": 1,
                           "subscriptions": 1, "control_outbox": 1},
    "relayforge_ingestion": {"idempotency_registry": 1, "events": 1,
                             "ingestion_outbox": 1},
    "relayforge_delivery": {"routing_progress": 1, "deliveries": 1,
                            "delivery_attempts": 1, "delivery_outbox": 2},
}


def run(command, **kwargs):
    timeout = kwargs.pop("timeout", 120)
    return subprocess.run(command, check=True, text=True, capture_output=True,
                          timeout=timeout, **kwargs)


def sql(service, database, statement):
    return run(COMPOSE + ["exec", "-T", service, "sh", "-c",
                          'PGPASSWORD="$POSTGRES_PASSWORD" psql -qAt -v ON_ERROR_STOP=1 '
                          '-U "$POSTGRES_USER" -d ' + database],
               input=statement + "\n").stdout.strip()


def wait_for(label, probe, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if probe():
            return
        time.sleep(1)
    raise AssertionError("Timed out waiting for " + label)


def healthy(service):
    found = run(COMPOSE + ["ps", "-q", service]).stdout.strip()
    if not found:
        return False
    return run(["docker", "inspect", "--format", "{{.State.Health.Status}}", found]).stdout.strip() \
        == "healthy"


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def migrate():
    for database, directory in MIGRATIONS.items():
        files = sorted(directory.glob("V*__*.sql"),
                       key=lambda path: int(path.name.split("__", 1)[0][1:]))
        assert files, database
        for migration in files:
            sql("source", database, migration.read_text())


def seed(prefix, base_url):
    organization, endpoint, subscription, event, delivery = [str(uuid4()) for _ in range(5)]
    stamp = "(current_date::timestamp AT TIME ZONE 'UTC') + interval '12 hours'"
    day = "current_date"
    sql("source", "relayforge_control", """
        BEGIN;
        INSERT INTO organizations (id, name) VALUES ('%(organization)s', '%(prefix)s owner');
        INSERT INTO endpoints (id, organization_id, name, base_url, path, auth_type)
        VALUES ('%(endpoint)s', '%(organization)s', '%(prefix)s endpoint', %(base_url)s,
                '/hook', 'NONE');
        INSERT INTO subscriptions (id, organization_id, endpoint_id, event_type)
        VALUES ('%(subscription)s', '%(organization)s', '%(endpoint)s', 'pitr.acceptance');
        INSERT INTO control_outbox (organization_id, source_revision, aggregate_type,
            aggregate_id, message_type, topic, partition_key, payload)
        VALUES ('%(organization)s', 1, 'endpoint', '%(endpoint)s', 'endpoint.changed.v1',
                'relayforge.configuration.changes.v1', '%(endpoint)s', '{}'::jsonb);
        COMMIT;
    """ % {"organization": organization, "endpoint": endpoint,
           "subscription": subscription, "prefix": prefix, "base_url": literal(base_url)})
    sql("source", "relayforge_ingestion", """
        BEGIN;
        INSERT INTO idempotency_registry (organization_id, idempotency_key, bucket_id,
            request_hash, request_hash_version, event_id, event_locator, accepted_day,
            accepted_at, expires_at)
        VALUES ('%(organization)s', '%(prefix)s-key', 0, decode(repeat('ab', 32), 'hex'), 1,
            '%(event)s', '%(prefix)s-locator', %(day)s, %(stamp)s,
            %(stamp)s + interval '2160 hours');
        INSERT INTO events (accepted_day, id, organization_id, event_type, schema_version,
            payload, payload_hash, idempotency_key, accepted_at)
        VALUES (%(day)s, '%(event)s', '%(organization)s', 'pitr.acceptance', 1,
            '{}'::jsonb, decode(repeat('ab', 32), 'hex'), '%(prefix)s-key', %(stamp)s);
        INSERT INTO ingestion_outbox (organization_id, accepted_day, event_id,
            message_type, topic, partition_key, payload)
        VALUES ('%(organization)s', %(day)s, '%(event)s', 'event.accepted.v1',
            'relayforge.event.accepted.v1', '%(event)s', '{}'::jsonb);
        COMMIT;
    """ % {"organization": organization, "event": event, "prefix": prefix,
           "day": day, "stamp": stamp})
    sql("source", "relayforge_delivery", """
        BEGIN;
        INSERT INTO routing_progress (accepted_day, event_id, organization_id,
            event_accepted_at, ingestion_shard_id, delivery_shard_id, status)
        VALUES (%(day)s, '%(event)s', '%(organization)s', %(stamp)s, 'ing-a', 'dlv-a',
            'ROUTED');
        INSERT INTO deliveries (accepted_day, id, organization_id, event_id, endpoint_id,
            subscription_id, status, attempt_count, completed_at)
        VALUES (%(day)s, '%(delivery)s', '%(organization)s', '%(event)s', '%(endpoint)s',
            '%(subscription)s', 'SUCCEEDED', 1, %(stamp)s);
        INSERT INTO delivery_attempts (accepted_day, organization_id, delivery_id,
            attempt_number, started_at, finished_at, outcome, http_status)
        VALUES (%(day)s, '%(organization)s', '%(delivery)s', 1, %(stamp)s, %(stamp)s,
            'SUCCEEDED', 204);
        COMMIT;
    """ % {"organization": organization, "endpoint": endpoint,
           "subscription": subscription, "event": event, "delivery": delivery,
           "day": day, "stamp": stamp})
    return organization


def verify_rows(organization, expected):
    for database, tables in TABLES.items():
        statements = "\n".join("SELECT count(*) FROM " + table +
                               " WHERE " + ("id" if table == "organizations"
                                           else "organization_id") + " = '" +
                               organization + "';" for table in tables)
        counts = sql("restored", database, statements).splitlines()
        assert counts == [str(count if expected else 0) for count in tables.values()], \
            (database, counts)


def main():
    try:
        run(COMPOSE + ["up", "-d", "source"])
        wait_for("source PostgreSQL", lambda: healthy("source"))
        migrate()
        run(COMPOSE + ["exec", "-T", "source", "sh", "-c",
                       'gosu postgres pg_basebackup -U "$POSTGRES_USER" -D /backup '
                       '-Fp -X none --checkpoint=fast'], timeout=180)
        assert run(COMPOSE + ["exec", "-T", "source", "test", "-s",
                              "/backup/PG_VERSION"]).returncode == 0

        retained = seed("retained", settings()["WEBHOOK_SIMULATOR_URL"])
        assert sql("source", "postgres",
                   "SELECT pg_create_restore_point('relayforge_pitr_target');")
        excluded = seed("excluded", settings()["WEBHOOK_SIMULATOR_URL"])
        wal_file = sql("source", "postgres", "SELECT pg_walfile_name(pg_current_wal_lsn());")
        sql("source", "postgres", "SELECT pg_switch_wal();")
        wait_for("archived WAL after restore point", lambda: subprocess.run(
            COMPOSE + ["exec", "-T", "source", "test", "-s", "/archive/" + wal_file],
            capture_output=True, timeout=20).returncode == 0)

        run(COMPOSE + ["up", "-d", "--no-deps", "restored"])
        wait_for("restored PostgreSQL", lambda: healthy("restored"))
        wait_for("restore promotion", lambda:
                 sql("restored", "postgres", "SELECT pg_is_in_recovery();") == "f")
        verify_rows(retained, 1)
        verify_rows(excluded, 0)
        print("Physical backup and archived WAL restored control, ingestion, and delivery state")
    finally:
        run(COMPOSE + ["down", "-v", "--remove-orphans"])


if __name__ == "__main__":
    main()

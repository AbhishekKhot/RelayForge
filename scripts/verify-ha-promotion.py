"""Exercise fenced promotion in an isolated PostgreSQL Compose project."""

import subprocess
import time
from uuid import uuid4


PROJECT = "relayforge-promotion-" + uuid4().hex[:8]
COMPOSE = ["docker", "compose", "-p", PROJECT, "-f", "compose.yaml",
           "-f", "deploy/ingestion-ha/compose.yaml"]


def run(command, **kwargs):
    return subprocess.run(command, check=True, text=True, capture_output=True,
                          timeout=120, **kwargs)


def sql(service, statement):
    result = run(COMPOSE + ["exec", "-T", service, "sh", "-c",
                            'PGPASSWORD="$POSTGRES_PASSWORD" psql -qAt -v ON_ERROR_STOP=1 '
                            '-U "$POSTGRES_USER" -d relayforge_ingestion'],
                 input=statement + "\n")
    return result.stdout.strip()


def container(service):
    return run(COMPOSE + ["ps", "-a", "-q", service]).stdout.strip()


def inspect(container_id, field):
    return run(["docker", "inspect", "--format", field, container_id]).stdout.strip()


def wait_for(label, probe, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = probe()
        if result:
            return result
        time.sleep(1)
    raise AssertionError("Timed out waiting for " + label)


def healthy(service):
    found = container(service)
    return bool(found and inspect(found, "{{.State.Health.Status}}") == "healthy")


def main():
    try:
        run(COMPOSE + ["up", "-d", "ingestion-standby"])
        wait_for("ingestion primary", lambda: healthy("ingestion-primary"))
        wait_for("ingestion standby", lambda: healthy("ingestion-standby"))
        run(COMPOSE + ["run", "--rm", "--no-deps", "ingestion-ha-enable"])
        assert sql("ingestion-primary", """
            SELECT count(*) FROM pg_stat_replication
            WHERE application_name = 'ingestion_standby' AND sync_state = 'sync';
        """) == "1"

        marker = uuid4().hex
        sql("ingestion-primary", """
            CREATE TABLE failover_probe (id text PRIMARY KEY);
            INSERT INTO failover_probe (id) VALUES ('%s');
        """ % marker)
        acknowledged_lsn = sql("ingestion-primary", "SELECT pg_current_wal_flush_lsn();")
        wait_for("standby WAL coverage", lambda: sql("ingestion-standby", """
            SELECT COALESCE(pg_last_wal_receive_lsn() >= '%s'::pg_lsn, false)
                AND COALESCE(pg_last_wal_replay_lsn() >= '%s'::pg_lsn, false);
        """ % (acknowledged_lsn, acknowledged_lsn)) == "t")
        assert sql("ingestion-standby", "SELECT count(*) FROM failover_probe WHERE id = '%s';"
                   % marker) == "1"
        assert sql("ingestion-standby", "SELECT pg_is_in_recovery();") == "t"

        old_primary = container("ingestion-primary")
        assert old_primary
        run(COMPOSE + ["exec", "-T", "ingestion-primary", "sh", "-c",
                       'touch "$PGDATA/.relayforge-fenced"'])
        run(COMPOSE + ["stop", "-t", "1", "ingestion-primary"])
        assert inspect(old_primary, "{{.State.Running}}") == "false"

        assert sql("ingestion-standby", "SELECT pg_promote(true, 60);") == "t"
        assert sql("ingestion-standby", "SELECT pg_is_in_recovery();") == "f"
        assert sql("ingestion-standby", "SELECT count(*) FROM failover_probe WHERE id = '%s';"
                   % marker) == "1"
        assert sql("ingestion-standby", """
            SELECT current_setting('fsync') = 'on'
                AND current_setting('synchronous_commit') = 'on'
                AND current_setting('synchronous_standby_names') <> ''
                AND EXISTS (SELECT 1 FROM pg_stat_replication
                            WHERE application_name = 'ingestion_standby'
                                AND sync_state IN ('sync', 'quorum'));
        """) == "f"

        subprocess.run(COMPOSE + ["start", "ingestion-primary"], text=True,
                       capture_output=True, timeout=30)
        wait_for("former primary to refuse restart", lambda:
                 inspect(old_primary, "{{.State.Status}}") == "exited")
        assert inspect(old_primary, "{{.State.ExitCode}}") == "1"
        assert "fenced old primary" in run(["docker", "logs", old_primary]).stderr
        print("Fenced promotion preserved the acknowledged commit and blocked old-primary restart")
    finally:
        run(COMPOSE + ["down", "-v", "--remove-orphans"])


if __name__ == "__main__":
    main()

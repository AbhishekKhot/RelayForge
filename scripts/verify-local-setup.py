"""Boot the documented developer stack from empty Compose volumes."""

import os
import subprocess
from uuid import uuid4


PROJECT = "relayforge-setup-" + uuid4().hex[:8]
COMPOSE = ["docker", "compose", "-p", PROJECT, "-f", "compose.yaml",
           "-f", "deploy/ingestion-ha/compose.yaml"]
HEALTHY = (
    "postgres", "kafka", "redis", "clickhouse",
    "ingestion-primary", "ingestion-standby", "control-plane", "control-publisher",
    "ingestion", "ingestion-publisher", "router", "delivery-api",
    "delivery-worker", "delivery-publisher", "retry-scheduler",
    "analytics-consumer", "analytics-api",
)
COMPLETED = ("kafka-data-init", "kafka-init", "ingestion-ha-enable")


def run(command, **kwargs):
    return subprocess.run(command, check=True, text=True, capture_output=True,
                          timeout=kwargs.pop("timeout", 120), **kwargs)


def inspect(service, field):
    container = run(COMPOSE + ["ps", "-a", "-q", service]).stdout.strip()
    assert container, service + " has no container"
    return run(["docker", "inspect", "--format", field, container]).stdout.strip()


def ingestion_sql(statement):
    return run(COMPOSE + ["exec", "-T", "ingestion-primary", "sh", "-c",
                          'PGPASSWORD="$POSTGRES_PASSWORD" psql -qAt '
                          '-U "$POSTGRES_USER" -d relayforge_ingestion'],
               input=statement + "\n").stdout.strip()


def verify_stack():
    for service in HEALTHY:
        assert inspect(service, "{{.State.Health.Status}}") == "healthy", service
    assert inspect("webhook-simulator", "{{.State.Running}}") == "true"
    for service in COMPLETED:
        assert inspect(service, "{{.State.ExitCode}}") == "0", service
    assert ingestion_sql("SELECT count(*) FROM pg_stat_replication "
                         "WHERE application_name = 'ingestion_standby' "
                         "AND sync_state = 'sync';") == "1"


def verify_delivery(environment):
    subprocess.run(["python3", "scripts/verify-j1.py"], check=True,
                   env={**environment, "COMPOSE_PROJECT_NAME": PROJECT}, timeout=300)


def main():
    environment = {**os.environ, "COMPOSE_PARALLEL_LIMIT": "2"}
    try:
        run(COMPOSE + ["config", "--quiet"])
        run(COMPOSE + ["up", "-d", "--build", "--wait", "--wait-timeout", "600"],
            env=environment, timeout=1200)
        verify_stack()
        verify_delivery(environment)
        accepted = int(ingestion_sql("SELECT count(*) FROM events;"))
        assert accepted > 0

        run(COMPOSE + ["down"])
        run(COMPOSE + ["up", "-d", "--wait", "--wait-timeout", "600"],
            env=environment, timeout=900)
        verify_stack()
        assert int(ingestion_sql("SELECT count(*) FROM events;")) >= accepted
        verify_delivery(environment)
        print("Documented HA Compose clean boot, restart, and delivery passed")
    except (subprocess.CalledProcessError, AssertionError):
        print(run(COMPOSE + ["ps", "-a"]).stdout)
        raise
    finally:
        run(COMPOSE + ["down", "-v", "--remove-orphans"])


if __name__ == "__main__":
    main()

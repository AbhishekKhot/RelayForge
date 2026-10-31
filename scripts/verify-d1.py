"""Verify control and delivery outbox publication on the Compose stack."""

import json
import subprocess
import time
from uuid import uuid4


COMPOSE = ["docker", "compose"]


def run(command, **kwargs):
    return subprocess.run(command, check=True, text=True, timeout=120, **kwargs)


def query(database, statement):
    result = run(COMPOSE + ["exec", "-T", "postgres", "sh", "-c",
                            'psql -qAt -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$1"',
                            "sh", database], input=statement + "\n", capture_output=True)
    return result.stdout.strip()


def wait_published(database, table, identifier):
    assert table in {"control_outbox", "delivery_outbox"}
    for _ in range(30):
        status = query(database, f"SELECT status FROM {table} WHERE id = '{identifier}';")
        if status == "PUBLISHED":
            return
        assert status in {"PENDING", "IN_FLIGHT"}, status
        time.sleep(2)
    raise AssertionError(f"{table} row did not publish")


def consume(topic):
    result = run(COMPOSE + ["exec", "-T", "kafka",
                            "/opt/kafka/bin/kafka-console-consumer.sh",
                            "--bootstrap-server", "kafka:9092", "--topic", topic,
                            "--from-beginning", "--max-messages", "1", "--timeout-ms", "10000"],
                 capture_output=True)
    envelope = json.loads(result.stdout.strip())
    assert envelope["message_id"].startswith("msg_")
    assert envelope["schema_version"] == 1
    assert envelope["correlation_id"] == envelope["message_id"]
    assert envelope["organization_id"] and envelope["aggregate_id"]
    assert isinstance(envelope["payload"], dict)


def verify_control():
    run(COMPOSE + ["stop", "control-publisher"])
    try:
        run(["python3", "scripts/verify-b3.py"])
        identifier = query("relayforge_control", """
            SELECT id FROM control_outbox WHERE status = 'PENDING'
            ORDER BY created_at DESC LIMIT 1;
        """)
        assert identifier
    finally:
        run(COMPOSE + ["start", "control-publisher"])
    wait_published("relayforge_control", "control_outbox", identifier)
    consume("relayforge.config.changed.v1")


def verify_delivery():
    identifier = uuid4()
    organization = uuid4()
    event = uuid4()
    delivery = uuid4()
    run(COMPOSE + ["stop", "delivery-publisher"])
    try:
        query("relayforge_delivery", f"""
            INSERT INTO delivery_outbox (id, organization_id, accepted_day, event_id,
                delivery_id, message_type, topic, partition_key, payload)
            VALUES ('{identifier}', '{organization}', current_date, '{event}', '{delivery}',
                'delivery.ready.v1', 'relayforge.delivery.ready.v1', '{delivery}',
                '{{"delivery_id":"{delivery}"}}'::jsonb);
        """)
        assert query("relayforge_delivery",
                     f"SELECT status FROM delivery_outbox WHERE id = '{identifier}';") == "PENDING"
    finally:
        run(COMPOSE + ["start", "delivery-publisher"])
    wait_published("relayforge_delivery", "delivery_outbox", identifier)
    consume("relayforge.delivery.ready.v1")


if __name__ == "__main__":
    verify_control()
    verify_delivery()
    print("D1 Compose outbox stop, recovery, and Kafka publication checks passed")

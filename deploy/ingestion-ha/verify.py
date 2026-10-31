"""Verify synchronous-standby event acceptance on the existing Compose stack."""

import json
import subprocess
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4


BASE = ["docker", "compose", "-f", "compose.yaml"]
HA = BASE + ["-f", "deploy/ingestion-ha/compose.yaml"]


def run(command, **kwargs):
    return subprocess.run(command, check=True, text=True, **kwargs)


def wait_healthy(service):
    for _ in range(120):
        container = run(HA + ["ps", "-q", service], capture_output=True).stdout.strip()
        if container:
            status = run(["docker", "inspect", "--format", "{{.State.Health.Status}}", container],
                         capture_output=True).stdout.strip()
            if status == "healthy":
                return
        time.sleep(2)
    raise AssertionError(f"{service} did not become healthy")


def post(base, path, body, headers):
    request = Request(base + path, json.dumps(body).encode(),
                      {"Content-Type": "application/json", **headers}, method="POST")
    try:
        with urlopen(request, timeout=15) as response:
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


def get(base, path, headers):
    request = Request(base + path, headers=headers, method="GET")
    try:
        with urlopen(request, timeout=15) as response:
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


def count(table, organization_id, status=None):
    assert table in {"events", "ingestion_outbox"}
    assert status is None or (table == "ingestion_outbox" and status == "PUBLISHED")
    status_filter = " AND status = 'PUBLISHED'" if status else ""
    result = run(HA + ["exec", "-T", "ingestion-primary", "sh", "-c",
                       'PGPASSWORD="$POSTGRES_PASSWORD" psql -U "$POSTGRES_USER" '
                       '-d relayforge_ingestion -At'],
                 input=f"SELECT count(*) FROM {table} WHERE organization_id = '{organization_id}'"
                       f"{status_filter};\n",
                 capture_output=True)
    return int(result.stdout.strip())


def verify():
    env = dict(line.split("=", 1) for line in Path(".env").read_text().splitlines()
               if line and not line.startswith("#") and "=" in line)
    control = f"http://127.0.0.1:{env['CONTROL_API_HOST_PORT']}"
    ingestion = f"http://127.0.0.1:{env['INGESTION_API_HOST_PORT']}"
    admin = {"X-Provisioning-Credential": env["TRUSTED_PROVISIONING_CREDENTIAL"],
             "Idempotency-Key": str(uuid4())}
    status, organization = post(control, "/v1/organizations", {"name": "C1 acceptance"}, admin)
    assert status == 201, (status, organization)
    organization_id = organization["data"]["organization_id"]
    bearer = {"Authorization": "Bearer " + organization["data"]["api_key"]}
    key = "c1-" + uuid4().hex
    body = {"event_type": "acceptance.test", "schema_version": 1,
            "data": {"acceptance": "c1"}}
    status, disabled = post(ingestion, "/v1/events", body,
                            {**bearer, "Idempotency-Key": key})
    assert status == 503, (status, disabled)
    run(HA + ["up", "-d", "ingestion", "ingestion-publisher"])
    wait_healthy("ingestion-primary")
    wait_healthy("ingestion-standby")
    wait_healthy("ingestion")
    status, first = post(ingestion, "/v1/events", body,
                         {**bearer, "Idempotency-Key": key})
    assert status == 202, (status, first)
    event_id = first["data"]["event_id"]
    status, owned = get(ingestion, f"/v1/events/{event_id}", bearer)
    assert status == 200 and owned["data"]["event_id"] == event_id, (status, owned)
    assert owned["data"]["payload"] == body["data"]
    assert owned["data"]["delivery_summary"]["status"] == "PENDING"
    other_admin = {**admin, "Idempotency-Key": str(uuid4())}
    status, other = post(control, "/v1/organizations", {"name": "C2 other"}, other_admin)
    assert status == 201, status
    other_bearer = {"Authorization": "Bearer " + other["data"]["api_key"]}
    status, _ = get(ingestion, f"/v1/events/{event_id}", other_bearer)
    assert status == 404
    status, _ = get(ingestion, "/v1/events/invalid-locator", bearer)
    assert status == 404
    status, repeat = post(ingestion, "/v1/events", body,
                          {**bearer, "Idempotency-Key": key})
    assert status == 202 and repeat["data"]["event_id"] == first["data"]["event_id"], (status, repeat)
    status, conflict = post(ingestion, "/v1/events",
                            {**body, "data": {"acceptance": "changed"}},
                            {**bearer, "Idempotency-Key": key})
    assert status == 409, (status, conflict)
    assert count("events", organization_id) == 1
    assert count("ingestion_outbox", organization_id) == 1
    for _ in range(30):
        if count("ingestion_outbox", organization_id, "PUBLISHED") == 1:
            break
        time.sleep(2)
    assert count("ingestion_outbox", organization_id, "PUBLISHED") == 1
    message = run(HA + ["exec", "-T", "kafka",
                        "/opt/kafka/bin/kafka-console-consumer.sh",
                        "--bootstrap-server", "kafka:9092",
                        "--topic", "relayforge.event.accepted.v1",
                        "--from-beginning", "--max-messages", "1", "--timeout-ms", "10000"],
                  capture_output=True).stdout.strip()
    envelope = json.loads(message)
    assert envelope["message_id"].startswith("msg_")
    assert envelope["schema_version"] == 1
    assert envelope["organization_id"] == organization_id
    assert envelope["payload"]["event_id"] == first["data"]["event_id"]
    run(HA + ["stop", "ingestion-standby"])
    failed_key = "c1-" + uuid4().hex
    status, failure = post(ingestion, "/v1/events", body,
                           {**bearer, "Idempotency-Key": failed_key})
    assert status == 503, (status, failure)
    assert count("events", organization_id) == 1
    run(HA + ["start", "ingestion-standby"])
    wait_healthy("ingestion-standby")
    for _ in range(30):
        status, recovered = post(ingestion, "/v1/events", body,
                                 {**bearer, "Idempotency-Key": failed_key})
        if status == 202:
            break
        assert status == 503, (status, recovered)
        time.sleep(2)
    assert status == 202, (status, recovered)
    assert count("events", organization_id) == 2
    assert count("ingestion_outbox", organization_id) == 2
    print("C1 HA event acceptance, idempotency, and standby-loss checks passed")


def restore_base():
    run(HA + ["stop", "ingestion", "ingestion-publisher", "ingestion-standby", "ingestion-primary"])
    run(HA + ["rm", "-f", "ingestion-ha-enable", "ingestion-standby", "ingestion-primary"])
    run(BASE + ["up", "-d", "--force-recreate", "ingestion", "ingestion-publisher"])


if __name__ == "__main__":
    try:
        verify()
    finally:
        restore_base()

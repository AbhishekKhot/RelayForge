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
    status_filter = " AND message_type = 'event.accepted.v1'" if table == "ingestion_outbox" else ""
    if status:
        status_filter += " AND status = 'PUBLISHED'"
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
    assert owned["data"]["delivery_summary"]["status"] in {"PENDING", "NO_SUBSCRIBERS"}
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
    verify_routing(env, control, ingestion)
    run(["python3", "scripts/verify-e2.py"])
    run(["python3", "scripts/verify-g1.py"])
    run(["python3", "scripts/verify-g2.py"])
    run(["python3", "scripts/verify-h2.py"])
    run(["python3", "scripts/verify-h3.py"])
    run(["python3", "scripts/verify-i1.py"])
    print("C1 HA event acceptance, idempotency, and standby-loss checks passed")


def delivery_count(table, organization_id):
    assert table in {"routing_targets", "deliveries", "delivery_work", "delivery_outbox"}
    message_filter = " AND message_type = 'delivery.ready.v1'" if table == "delivery_outbox" else ""
    result = run(HA + ["exec", "-T", "postgres", "sh", "-c",
                       'psql -qAt -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d relayforge_delivery'],
                 input=f"SELECT count(*) FROM {table} WHERE organization_id = '{organization_id}'"
                       f"{message_filter};\n",
                 capture_output=True)
    return int(result.stdout.strip())


def delivery_sql(statement):
    result = run(HA + ["exec", "-T", "postgres", "sh", "-c",
                       'psql -qAt -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d relayforge_delivery'],
                 input=statement + "\n", capture_output=True)
    return result.stdout.strip()


def verify_expired_lease_recovery():
    organization_id = uuid4()
    event = uuid4()
    endpoint = uuid4()
    delivery = uuid4()
    token = uuid4()
    delivery_sql("""BEGIN;
        INSERT INTO routing_progress (accepted_day, event_id, organization_id,
            event_accepted_at, ingestion_shard_id, delivery_shard_id, status)
        VALUES (current_date, '%s', '%s',
            current_date::timestamp AT TIME ZONE 'UTC', 'ing-a', 'dlv-a', 'ROUTED');
        INSERT INTO deliveries (accepted_day, id, organization_id, event_id, endpoint_id,
            status, lease_owner, lease_token, lease_expires_at, processing_deadline_at,
            claim_count)
        VALUES (current_date, '%s', '%s', '%s', '%s', 'IN_PROGRESS', 'acceptance',
            '%s', clock_timestamp() - interval '1 second',
            clock_timestamp() + interval '3 minutes', 1);
        INSERT INTO delivery_work (delivery_id, accepted_day, organization_id, endpoint_id,
            status, event_expires_at, next_attempt_at, lease_owner, lease_token,
            lease_expires_at, processing_deadline_at, claim_count)
        VALUES ('%s', current_date, '%s', '%s', 'IN_PROGRESS',
            clock_timestamp() + interval '90 days', clock_timestamp(), 'acceptance',
            '%s', clock_timestamp() - interval '1 second',
            clock_timestamp() + interval '3 minutes', 1);
        COMMIT;""" % (event, organization_id, delivery, organization_id, event,
                      endpoint, token, delivery, organization_id, endpoint, token))
    for _ in range(30):
        state = delivery_sql("""SELECT work.status || ':' || delivery.status || ':' ||
            (work.lease_token IS NULL)::text || ':' || (delivery.lease_token IS NULL)::text
            FROM delivery_work work JOIN deliveries delivery
              ON delivery.accepted_day = work.accepted_day AND delivery.id = work.delivery_id
            WHERE work.delivery_id = '%s';""" % delivery)
        if state == "PENDING:PENDING:true:true":
            return
        time.sleep(2)
    raise AssertionError("Expired delivery lease was not recovered")


def wait_routing(ingestion, locator, bearer, expected_status, expected_count):
    for _ in range(60):
        status, event = get(ingestion, f"/v1/events/{locator}", bearer)
        if status == 200:
            summary = event["data"]["delivery_summary"]
            if summary["status"] == expected_status and summary["delivery_count"] == expected_count:
                return
        time.sleep(2)
    raise AssertionError("Event routing did not reach the expected outcome")


def wait_router_caught_up(shard):
    for _ in range(45):
        result = run(HA + ["exec", "-T", "kafka",
                           "/opt/kafka/bin/kafka-consumer-groups.sh", "--bootstrap-server", "kafka:9092",
                           "--describe", "--group", f"relayforge-router-events-{shard}"],
                     capture_output=True)
        lines = [parts for parts in (line.split() for line in result.stdout.splitlines())
                 if len(parts) >= 6 and parts[1] == "relayforge.event.accepted.v1"]
        if lines and all(len(parts) >= 6 and parts[5] == "0" for parts in lines):
            return
        time.sleep(2)
    raise AssertionError("Router consumer did not catch up")


def verify_routing(env, control, ingestion):
    run(HA + ["stop", "delivery-worker"])
    admin = {"X-Provisioning-Credential": env["TRUSTED_PROVISIONING_CREDENTIAL"],
             "Idempotency-Key": str(uuid4())}
    status, organization = post(control, "/v1/organizations", {"name": "D3 routing acceptance"}, admin)
    assert status == 201
    organization_id = organization["data"]["organization_id"]
    key_id = organization["data"]["api_key"].split("_")[2]
    status, rotated = post(control, f"/v1/api-keys/{key_id}/rotate",
                           {"scopes": ["events:write", "events:read", "endpoints:write"]}, admin)
    assert status == 201
    bearer = {"Authorization": "Bearer " + rotated["data"]["api_key"]}
    endpoint = {"name": "D3 target", "destination": {
        "base_url": env["WEBHOOK_SIMULATOR_URL"], "path": "/hook", "query_params": {},
        "headers": {}, "metadata": {}}, "authentication": {"type": "NONE"},
        "event_types": ["acceptance.d3"], "delivery_policy": {
            "timeout_ms": 10000, "max_attempts": 3, "max_concurrency": 2}}
    for index in range(2):
        status, _ = post(control, "/v1/endpoints", {**endpoint, "name": f"D3 target {index}"}, bearer)
        assert status == 201
    status, snapshot = get(control, f"/internal/v1/organizations/{organization_id}"
                           "/configuration/endpoints?limit=100",
                           {"X-RelayForge-Router-Config-Credential": env["ROUTER_CONFIG_CREDENTIAL"]})
    assert status == 200 and len(snapshot["data"]) == 2
    assert "base_url" not in json.dumps(snapshot) and "auth_config" not in json.dumps(snapshot)

    body = {"event_type": "acceptance.d3", "schema_version": 1, "data": {"routing": "two"}}
    status, accepted = post(ingestion, "/v1/events", body,
                            {**bearer, "Idempotency-Key": "d3-" + uuid4().hex})
    assert status == 202
    locator = accepted["data"]["event_id"]
    wait_routing(ingestion, locator, bearer, "ROUTED", 2)
    assert delivery_count("routing_targets", organization_id) == 2
    assert delivery_count("deliveries", organization_id) == 2
    assert delivery_count("delivery_work", organization_id) == 2
    assert delivery_count("delivery_outbox", organization_id) == 2

    result = run(HA + ["exec", "-T", "ingestion-primary", "sh", "-c",
                       'psql -qAt -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d relayforge_ingestion'],
                 input="""SELECT jsonb_build_object('message_id', 'msg_' || id::text,
                    'message_type', message_type, 'schema_version', 1, 'occurred_at', created_at,
                    'organization_id', organization_id, 'aggregate_id', event_id::text,
                    'correlation_id', 'msg_' || id::text, 'payload', payload)::text
                    FROM ingestion_outbox WHERE organization_id = '%s' AND partition_key = '%s'
                        AND message_type = 'event.accepted.v1';
                    """ % (organization_id, locator), capture_output=True)
    duplicate = result.stdout.strip()
    assert duplicate
    run(HA + ["exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-producer.sh",
              "--bootstrap-server", "kafka:9092", "--topic", "relayforge.event.accepted.v1",
              "--property", "parse.key=true", "--property", "key.separator=|"],
        input=locator + "|" + duplicate + "\n", capture_output=True)
    wait_router_caught_up(env["DELIVERY_SHARD_ID"])
    assert delivery_count("deliveries", organization_id) == 2
    assert delivery_count("delivery_outbox", organization_id) == 2

    status, zero = post(ingestion, "/v1/events",
                        {"event_type": "acceptance.d3.none", "schema_version": 1, "data": {}},
                        {**bearer, "Idempotency-Key": "d3-" + uuid4().hex})
    assert status == 202
    wait_routing(ingestion, zero["data"]["event_id"], bearer, "NO_SUBSCRIBERS", 0)
    assert delivery_count("deliveries", organization_id) == 2
    run(HA + ["start", "delivery-worker"])
    wait_healthy("delivery-worker")
    verify_expired_lease_recovery()
    print("D3 routing and E1 expired-lease recovery checks passed")


def restore_base():
    run(HA + ["stop", "ingestion", "ingestion-publisher", "ingestion-standby", "ingestion-primary"])
    run(HA + ["rm", "-f", "ingestion-ha-enable", "ingestion-standby", "ingestion-primary"])
    run(BASE + ["up", "-d", "--force-recreate", "ingestion", "ingestion-publisher"])


if __name__ == "__main__":
    try:
        verify()
    finally:
        restore_base()

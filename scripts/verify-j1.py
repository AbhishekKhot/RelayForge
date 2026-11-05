"""Verify one complete event-to-webhook-to-analytics flow on Compose."""

import base64
import json
import re
import subprocess
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
from uuid import UUID, uuid4

from verify_support import get, post, settings


COMPOSE = ["docker", "compose"]
DATABASES = {"relayforge_control", "relayforge_ingestion", "relayforge_delivery"}


def sql(database, statement):
    assert database in DATABASES
    result = subprocess.run(
        COMPOSE + ["exec", "-T", "postgres", "sh", "-c",
                   'psql -qAt -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$1"',
                   "sh", database],
        input=statement + "\n", text=True, capture_output=True, check=True, timeout=20)
    return result.stdout.strip()


def clickhouse(statement):
    result = subprocess.run(
        COMPOSE + ["exec", "-T", "clickhouse", "sh", "-c",
                   'clickhouse-client --user "$CLICKHOUSE_USER" '
                   '--password "$CLICKHOUSE_PASSWORD" --query "$1"',
                   "sh", statement],
        text=True, capture_output=True, check=True, timeout=20)
    return result.stdout.strip()


def wait_for(label, probe, timeout=120):
    deadline = time.monotonic() + timeout
    while True:
        found = probe()
        if found:
            return found
        if time.monotonic() >= deadline:
            raise AssertionError(f"Timed out waiting for {label}")
        time.sleep(1)


def kafka_message(broker, message_id, organization, event_id, locator):
    command = COMPOSE + [
        "exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-consumer.sh",
        "--bootstrap-server", broker, "--topic", "relayforge.event.accepted.v1",
        "--from-beginning", "--timeout-ms", "10000",
    ]
    result = subprocess.run(command, text=True, capture_output=True, timeout=40)
    messages = []
    for line in result.stdout.splitlines():
        try:
            messages.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    matches = [message for message in messages if message.get("message_id") == message_id]
    assert len(matches) == 1, f"Kafka accepted message count: {len(matches)}"
    message = matches[0]
    assert message["message_type"] == "event.accepted.v1"
    assert message["organization_id"] == organization
    assert message["aggregate_id"] == event_id
    assert message["payload"]["event_id"] == locator


def main():
    env = settings()
    control = f"http://127.0.0.1:{env['CONTROL_API_HOST_PORT']}"
    ingestion = f"http://127.0.0.1:{env['INGESTION_API_HOST_PORT']}"
    delivery_api = f"http://127.0.0.1:{env['DELIVERY_API_HOST_PORT']}"
    analytics = f"http://127.0.0.1:{env['ANALYTICS_API_HOST_PORT']}"
    simulator = f"http://127.0.0.1:{env['WEBHOOK_SIMULATOR_HOST_PORT']}"
    run_id = uuid4().hex
    event_type = "order.created"
    hook_path = "/j1-" + run_id
    idempotency_key = "j1-" + run_id
    payload_marker = str(uuid4())
    started = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(minutes=1)

    admin = {"X-Provisioning-Credential": env["TRUSTED_PROVISIONING_CREDENTIAL"],
             "Idempotency-Key": str(uuid4())}
    status, owner = post(control, "/v1/organizations", {"name": "J1 " + run_id}, admin)
    assert status == 201, f"Organization provisioning returned {status}"
    organization = str(UUID(owner["data"]["organization_id"]))
    initial_key_id = owner["data"]["api_key"].split("_")[2]
    scopes = ["events:write", "events:read", "endpoints:write",
              "deliveries:read", "analytics:read"]
    status, rotated = post(control, f"/v1/api-keys/{initial_key_id}/rotate",
                           {"scopes": scopes}, admin)
    assert status == 201, f"API key rotation returned {status}"
    bearer = {"Authorization": "Bearer " + rotated["data"]["api_key"]}

    status, mapping = post(simulator, "/__admin/mappings", {
        "request": {"method": "POST", "urlPath": hook_path},
        "response": {"status": 204},
    }, {})
    assert status == 201 and mapping["id"], f"WireMock mapping returned {status}"
    status, endpoint = post(control, "/v1/endpoints", {
        "name": "J1 endpoint " + run_id,
        "destination": {"base_url": env["WEBHOOK_SIMULATOR_URL"],
                        "path": hook_path, "query_params": {},
                        "headers": {}, "metadata": {}},
        "authentication": {"type": "NONE"},
        "event_types": [event_type],
        "delivery_policy": {"timeout_ms": 3000, "max_attempts": 3,
                            "max_concurrency": 2},
    }, bearer)
    assert status == 201, f"Endpoint registration returned {status}"
    endpoint_id = str(UUID(endpoint["data"]["endpoint_id"]))
    wait_for("endpoint projection", lambda: sql("relayforge_delivery", f"""
        SELECT count(*) FROM subscription_projection
        WHERE organization_id = '{organization}' AND endpoint_id = '{endpoint_id}'
          AND event_type = '{event_type}' AND status = 'ACTIVE';
    """) == "1")

    status, accepted = post(ingestion, "/v1/events",
                            {"event_type": event_type, "data": {"check": payload_marker}},
                            {**bearer, "Idempotency-Key": idempotency_key})
    assert status == 202, f"Event ingestion returned {status}"
    locator = accepted["data"]["event_id"]
    assert re.fullmatch(r"evt_[A-Za-z0-9_-]+", locator), "Invalid event locator"
    event_row = sql("relayforge_ingestion", f"""
        SELECT id || '|' || event_type || '|' || payload ->> 'check'
        FROM events WHERE organization_id = '{organization}'
          AND idempotency_key = '{idempotency_key}';
    """)
    event_id, stored_type, stored_marker = event_row.split("|")
    event_id = str(UUID(event_id))
    assert (stored_type, stored_marker) == (event_type, payload_marker)
    assert sql("relayforge_ingestion", f"""
        SELECT count(*) FROM idempotency_registry
        WHERE organization_id = '{organization}' AND event_id = '{event_id}'
          AND event_locator = '{locator}';
    """) == "1"
    outbox = wait_for("published event outbox", lambda: sql("relayforge_ingestion", f"""
        SELECT id || '|' || status FROM ingestion_outbox
        WHERE event_id = '{event_id}' AND message_type = 'event.accepted.v1'
          AND status = 'PUBLISHED' AND published_at IS NOT NULL;
    """))
    outbox_id, published = outbox.split("|")
    assert published == "PUBLISHED"
    kafka_message(env["KAFKA_BOOTSTRAP_SERVERS"], "msg_" + str(UUID(outbox_id)),
                  organization, event_id, locator)

    delivery_row = wait_for("successful delivery", lambda: sql("relayforge_delivery", f"""
        SELECT delivery.id || '|' || delivery.status || '|' || delivery.attempt_count
        FROM deliveries delivery JOIN routing_progress progress
          ON progress.accepted_day = delivery.accepted_day
         AND progress.event_id = delivery.event_id
         AND progress.organization_id = delivery.organization_id
        WHERE progress.event_locator = '{locator}'
          AND delivery.organization_id = '{organization}'
          AND delivery.status = 'SUCCEEDED';
    """))
    delivery_id, delivery_status, attempts = delivery_row.split("|")
    delivery_id = str(UUID(delivery_id))
    assert (delivery_status, attempts) == ("SUCCEEDED", "1")
    assert sql("relayforge_delivery", f"""
        SELECT count(*) FROM routing_progress WHERE event_id = '{event_id}'
          AND organization_id = '{organization}' AND status = 'ROUTED';
    """) == "1"
    wait_for("published delivery-ready outbox", lambda: sql("relayforge_delivery", f"""
        SELECT count(*) FROM delivery_outbox WHERE delivery_id = '{delivery_id}'
          AND message_type = 'delivery.ready.v1' AND status = 'PUBLISHED'
          AND published_at IS NOT NULL;
    """) == "1")
    assert sql("relayforge_delivery", f"""
        SELECT attempt_number || '|' || outcome || '|' || http_status
        FROM delivery_attempts WHERE delivery_id = '{delivery_id}';
    """) == "1|SUCCEEDED|204"

    status, journal = post(simulator, "/__admin/requests/find", {
        "method": "POST", "url": hook_path,
        "bodyPatterns": [{"contains": delivery_id}],
    }, {})
    assert status == 200 and len(journal["requests"]) == 1
    received = journal["requests"][0]
    body = json.loads(base64.b64decode(received["bodyAsBase64"]))
    assert body["delivery_id"] == delivery_id
    assert body["event_id"] == locator
    assert body["event_type"] == event_type
    assert body["attempt_number"] == 1
    assert body["data"] == {"check": payload_marker}

    wait_for("ClickHouse accepted and success facts", lambda: clickhouse(f"""
        SELECT count() FROM relayforge_analytics.delivery_facts FINAL
        WHERE organization_id = '{organization}' AND event_id = '{event_id}'
          AND fact_type = 'event.accepted'
    """) == "1" and clickhouse(f"""
        SELECT count() FROM relayforge_analytics.delivery_facts FINAL
        WHERE organization_id = '{organization}' AND delivery_id = '{delivery_id}'
          AND fact_type = 'delivery.succeeded'
    """) == "1")

    status, event_view = get(ingestion, "/v1/events/" + locator, bearer)
    assert status == 200 and event_view["data"]["payload"] == {"check": payload_marker}
    assert event_view["data"]["delivery_summary"]["succeeded_count"] == 1
    status, detail = get(delivery_api, "/v1/deliveries/" + delivery_id, bearer)
    assert status == 200 and detail["data"]["delivery"]["status"] == "SUCCEEDED"
    assert detail["data"]["attempts"][0]["http_status"] == 204

    ended = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(minutes=1)
    window = urlencode({"from": started.isoformat().replace("+00:00", "Z"),
                        "to": ended.isoformat().replace("+00:00", "Z")})
    filters = window + "&" + urlencode({"status": "SUCCEEDED", "endpoint_id": endpoint_id,
                                        "limit": 10})
    status, page = get(delivery_api, "/v1/deliveries?" + filters, bearer)
    assert status == 200 and [row["delivery_id"] for row in page["data"]] == [delivery_id]
    status, overview = get(analytics, "/v1/analytics/overview?" + window, bearer)
    assert status == 200 and overview["data_as_of"]
    assert overview["data"]["events_accepted"] == 1
    assert overview["data"]["deliveries_created"] == 1
    assert overview["data"]["deliveries_succeeded"] == 1
    series_path = ("/v1/analytics/deliveries/timeseries?" + window + "&"
                   + urlencode({"interval": "hour", "endpoint_id": endpoint_id}))
    status, series = get(analytics, series_path, bearer)
    assert status == 200 and series["data_as_of"]
    points = series["data"]["points"]
    assert sum(point["deliveries_created"] for point in points) == 1
    assert sum(point["deliveries_succeeded"] for point in points) == 1
    print("J1 event, outbox, Kafka, routing, webhook, attempt, and analytics flow passed")


if __name__ == "__main__":
    main()

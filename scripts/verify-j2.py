"""Verify duplicate event and delivery messages have one external effect."""

import json
import subprocess
import time
from uuid import UUID, uuid4

from verify_support import get, post, post_status, query, settings


COMPOSE = ["docker", "compose", "-f", "compose.yaml",
           "-f", "deploy/ingestion-ha/compose.yaml"]


def ingestion_sql(statement):
    result = subprocess.run(
        COMPOSE + ["exec", "-T", "ingestion-primary", "sh", "-c",
                   'psql -qAt -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d relayforge_ingestion'],
        input=statement + "\n", text=True, capture_output=True, timeout=20)
    assert result.returncode == 0, f"Ingestion query failed: {result.stderr.strip()}"
    return result.stdout.strip()


def clickhouse(statement):
    result = subprocess.run(
        COMPOSE + ["exec", "-T", "clickhouse", "sh", "-c",
                   'clickhouse-client --user "$CLICKHOUSE_USER" '
                   '--password "$CLICKHOUSE_PASSWORD" --query "$1"', "sh", statement],
        text=True, capture_output=True, timeout=20)
    assert result.returncode == 0, f"ClickHouse query failed: {result.stderr.strip()}"
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


def publish_duplicate(topic, key, envelope):
    result = subprocess.run(
        COMPOSE + ["exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-producer.sh",
                   "--bootstrap-server", "kafka:9092", "--topic", topic,
                   "--property", "parse.key=true", "--property", "key.separator=|"],
        input=key + "|" + envelope + "\n", text=True, capture_output=True, timeout=20)
    assert result.returncode == 0, f"Kafka duplicate publish failed: {result.stderr.strip()}"


def consumer_caught_up(group, topic):
    result = subprocess.run(
        COMPOSE + ["exec", "-T", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh",
                   "--bootstrap-server", "kafka:9092", "--describe", "--group", group],
        text=True, capture_output=True, timeout=20)
    if result.returncode != 0:
        return False
    lines = [parts for parts in (line.split() for line in result.stdout.splitlines())
             if len(parts) >= 6 and parts[1] == topic]
    return bool(lines) and all(parts[5] == "0" for parts in lines)


def outbox_envelope(database, outbox_id):
    aggregate = "event_id" if database == "ingestion_outbox" else "delivery_id"
    statement = f"""SELECT jsonb_build_object(
        'message_id', 'msg_' || id::text, 'message_type', message_type,
        'schema_version', 1, 'occurred_at', created_at,
        'organization_id', organization_id, 'aggregate_id', {aggregate}::text,
        'correlation_id', 'msg_' || id::text, 'payload', payload)::text
        FROM {database} WHERE id = '{UUID(outbox_id)}';"""
    return ingestion_sql(statement) if database == "ingestion_outbox" else query(statement)


def main():
    env = settings()
    control = f"http://127.0.0.1:{env['CONTROL_API_HOST_PORT']}"
    ingestion = f"http://127.0.0.1:{env['INGESTION_API_HOST_PORT']}"
    delivery_api = f"http://127.0.0.1:{env['DELIVERY_API_HOST_PORT']}"
    simulator = f"http://127.0.0.1:{env['WEBHOOK_SIMULATOR_HOST_PORT']}"
    run_id = uuid4().hex
    hook_path = "/j2-duplicate-" + run_id
    key = "j2-" + run_id
    event_type = "acceptance.j2." + run_id
    admin = {"X-Provisioning-Credential": env["TRUSTED_PROVISIONING_CREDENTIAL"],
             "Idempotency-Key": str(uuid4())}
    status, owner = post(control, "/v1/organizations", {"name": "J2 " + run_id}, admin)
    assert status == 201
    organization = str(UUID(owner["data"]["organization_id"]))
    key_id = owner["data"]["api_key"].split("_")[2]
    status, rotated = post(control, f"/v1/api-keys/{key_id}/rotate",
                           {"scopes": ["events:write", "events:read", "endpoints:write",
                                       "deliveries:read", "analytics:read"]}, admin)
    assert status == 201
    bearer = {"Authorization": "Bearer " + rotated["data"]["api_key"]}
    status, mapping = post(simulator, "/__admin/mappings", {
        "request": {"method": "POST", "urlPath": hook_path},
        "response": {"status": 204}}, {})
    assert status == 201 and mapping["id"]
    status, endpoint = post(control, "/v1/endpoints", {
        "name": "J2 duplicate receiver " + run_id,
        "destination": {"base_url": env["WEBHOOK_SIMULATOR_URL"], "path": hook_path,
                        "query_params": {}, "headers": {}, "metadata": {}},
        "authentication": {"type": "NONE"}, "event_types": [event_type],
        "delivery_policy": {"timeout_ms": 3000, "max_attempts": 3,
                            "max_concurrency": 2}}, bearer)
    assert status == 201
    endpoint_id = str(UUID(endpoint["data"]["endpoint_id"]))
    wait_for("J2 endpoint projection", lambda: query(f"""
        SELECT count(*) FROM subscription_projection WHERE organization_id = '{organization}'
          AND endpoint_id = '{endpoint_id}' AND event_type = '{event_type}'
          AND status = 'ACTIVE';""") == "1")

    subprocess.run(COMPOSE + ["stop", "delivery-worker"], check=True, timeout=30)
    try:
        body = {"event_type": event_type, "data": {"check": run_id}}
        headers = {**bearer, "Idempotency-Key": key}
        status, accepted = post_status(ingestion, "/v1/events", body, headers)
        assert status == 202, (status, accepted)
        locator = accepted["data"]["event_id"]
        status, repeated = post_status(ingestion, "/v1/events", body, headers)
        assert status == 202 and repeated["data"]["event_id"] == locator
        status, conflict = post_status(ingestion, "/v1/events", {
            "event_type": event_type, "data": {"check": "changed"}}, headers)
        assert status == 409 and conflict["code"] == "BAD_REQUEST"
        event_row = ingestion_sql(f"""
            SELECT id || '|' || count(*) OVER () FROM events
            WHERE organization_id = '{organization}' AND idempotency_key = '{key}';""")
        event_id, event_count = event_row.split("|")
        assert event_count == "1"
        assert ingestion_sql(f"""SELECT count(*) FROM idempotency_registry
            WHERE organization_id = '{organization}' AND idempotency_key = '{key}'
              AND event_id = '{event_id}';""") == "1"
        accepted_outbox = wait_for("J2 published accepted outbox", lambda: ingestion_sql(f"""
            SELECT id FROM ingestion_outbox WHERE organization_id = '{organization}'
              AND event_id = '{event_id}' AND message_type = 'event.accepted.v1'
              AND status = 'PUBLISHED' AND published_at IS NOT NULL;"""))
        assert ingestion_sql(f"""SELECT count(*) FROM ingestion_outbox
            WHERE organization_id = '{organization}' AND event_id = '{event_id}'
              AND message_type = 'event.accepted.v1';""") == "1"
        delivery_id = wait_for("J2 routed delivery", lambda: query(f"""
            SELECT delivery.id FROM deliveries delivery JOIN routing_progress progress
              ON progress.accepted_day = delivery.accepted_day
             AND progress.event_id = delivery.event_id
             AND progress.organization_id = delivery.organization_id
            WHERE progress.event_locator = '{locator}'
              AND delivery.organization_id = '{organization}';"""))
        delivery_id = str(UUID(delivery_id))
        ready_outbox = wait_for("J2 published ready outbox", lambda: query(f"""
            SELECT id FROM delivery_outbox WHERE organization_id = '{organization}'
              AND delivery_id = '{delivery_id}' AND message_type = 'delivery.ready.v1'
              AND status = 'PUBLISHED' AND published_at IS NOT NULL;"""))
        assert query(f"""SELECT count(*) FROM deliveries WHERE organization_id = '{organization}'
            AND event_id = '{event_id}';""") == "1"
        assert query(f"""SELECT count(*) FROM delivery_outbox WHERE delivery_id = '{delivery_id}'
            AND message_type = 'delivery.ready.v1';""") == "1"
        publish_duplicate("relayforge.event.accepted.v1", locator,
                          outbox_envelope("ingestion_outbox", accepted_outbox))
        wait_for("router duplicate consumption", lambda: consumer_caught_up(
            f"relayforge-router-events-{env['DELIVERY_SHARD_ID']}",
            "relayforge.event.accepted.v1"))
        assert query(f"""SELECT count(*) FROM deliveries WHERE organization_id = '{organization}'
            AND event_id = '{event_id}';""") == "1"
        publish_duplicate("relayforge.delivery.ready.v1", delivery_id,
                          outbox_envelope("delivery_outbox", ready_outbox))
    finally:
        subprocess.run(COMPOSE + ["start", "delivery-worker"], check=True, timeout=30)

    wait_for("J2 successful delivery", lambda: query(f"""
        SELECT status FROM deliveries WHERE id = '{delivery_id}';""") == "SUCCEEDED")
    wait_for("worker duplicate consumption", lambda: consumer_caught_up(
        "relayforge-delivery-worker", "relayforge.delivery.ready.v1"))
    assert query(f"""SELECT attempt_number || '|' || outcome || '|' || http_status
        FROM delivery_attempts WHERE delivery_id = '{delivery_id}';""") == "1|SUCCEEDED|204"
    assert query(f"""SELECT count(*) FROM deliveries WHERE organization_id = '{organization}'
        AND event_id = '{event_id}';""") == "1"
    status, counted = post(simulator, "/__admin/requests/count",
                           {"method": "POST", "url": hook_path}, {})
    assert status == 200 and counted["count"] == 1, counted
    wait_for("J2 deduplicated analytics facts", lambda: clickhouse(f"""
        SELECT count() FROM relayforge_analytics.delivery_facts FINAL
        WHERE organization_id = '{organization}' AND event_id = '{event_id}'
          AND fact_type = 'event.accepted'""") == "1" and clickhouse(f"""
        SELECT count() FROM relayforge_analytics.delivery_facts FINAL
        WHERE organization_id = '{organization}' AND delivery_id = '{delivery_id}'
          AND fact_type = 'delivery.succeeded'""") == "1")
    status, event_view = get(ingestion, "/v1/events/" + locator, bearer)
    assert status == 200 and event_view["data"]["delivery_summary"]["succeeded_count"] == 1
    status, detail = get(delivery_api, "/v1/deliveries/" + delivery_id, bearer)
    assert status == 200 and detail["data"]["delivery"]["status"] == "SUCCEEDED"
    print("J2 duplicate API, accepted Kafka, and ready Kafka messages had one external effect")


if __name__ == "__main__":
    main()

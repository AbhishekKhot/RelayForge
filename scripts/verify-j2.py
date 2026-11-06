"""Verify failure and duplicate scenarios on the synchronous Compose stack."""

import base64
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


def receiver_case(control, ingestion, delivery_api, simulator, env, bearer, organization,
                  run_id, case, first_response, first_status, retry):
    event_type = f"acceptance.j2.{case}.{run_id}"
    hook_path = f"/j2-{case}-{run_id}"
    mapping = {"request": {"method": "POST", "urlPath": hook_path},
               "response": first_response}
    if retry:
        scenario = f"j2-{case}-{run_id}"
        mapping.update({"scenarioName": scenario, "requiredScenarioState": "Started",
                        "newScenarioState": "recovered"})
    status, created = post(simulator, "/__admin/mappings", mapping, {})
    assert status == 201 and created["id"]
    if retry:
        status, created = post(simulator, "/__admin/mappings", {
            "request": {"method": "POST", "urlPath": hook_path},
            "response": {"status": 204}, "scenarioName": scenario,
            "requiredScenarioState": "recovered"}, {})
        assert status == 201 and created["id"]
    status, endpoint = post(control, "/v1/endpoints", {
        "name": "J2 " + case + " " + run_id,
        "destination": {"base_url": env["WEBHOOK_SIMULATOR_URL"], "path": hook_path,
                        "query_params": {}, "headers": {}, "metadata": {}},
        "authentication": {"type": "NONE"}, "event_types": [event_type],
        "delivery_policy": {"timeout_ms": 3000, "max_attempts": 2,
                            "max_concurrency": 2}}, bearer)
    assert status == 201
    endpoint_id = str(UUID(endpoint["data"]["endpoint_id"]))
    wait_for(case + " projection", lambda: query(f"""
        SELECT count(*) FROM subscription_projection WHERE organization_id = '{organization}'
          AND endpoint_id = '{endpoint_id}' AND event_type = '{event_type}'
          AND status = 'ACTIVE';""") == "1")
    status, accepted = post_status(ingestion, "/v1/events", {
        "event_type": event_type, "data": {"check": case}},
        {**bearer, "Idempotency-Key": str(uuid4())})
    assert status == 202, (case, status, accepted)
    locator = accepted["data"]["event_id"]
    expected = "SUCCEEDED" if retry else "DEAD_LETTERED"
    delivery_id = wait_for(case + " final delivery", lambda: query(f"""
        SELECT delivery.id FROM deliveries delivery JOIN routing_progress progress
          ON progress.accepted_day = delivery.accepted_day
         AND progress.event_id = delivery.event_id
         AND progress.organization_id = delivery.organization_id
        WHERE progress.event_locator = '{locator}'
          AND delivery.organization_id = '{organization}'
          AND delivery.status = '{expected}';"""))
    delivery_id = str(UUID(delivery_id))
    event_id = query(f"""SELECT event_id FROM deliveries WHERE id = '{delivery_id}';""")
    assert str(UUID(event_id)) == event_id
    attempts = "2" if retry else "1"
    assert query(f"""SELECT attempt_count FROM deliveries
        WHERE id = '{delivery_id}';""") == attempts
    first_attempt = query(f"""SELECT outcome || '|' || COALESCE(http_status::text, '')
        FROM delivery_attempts WHERE delivery_id = '{delivery_id}' AND attempt_number = 1;""")
    assert first_attempt == (("RETRY_SCHEDULED" if retry else "DEAD_LETTERED")
                             + "|" + (str(first_status) if first_status else "")), (case, first_attempt)
    if retry:
        assert query(f"""SELECT outcome || '|' || http_status FROM delivery_attempts
            WHERE delivery_id = '{delivery_id}' AND attempt_number = 2;""") == "SUCCEEDED|204"
    assert query(f"""SELECT count(*) FROM deliveries WHERE organization_id = '{organization}'
        AND event_id = '{event_id}';""") == "1"
    assert query(f"""SELECT count(*) FROM delivery_outbox WHERE delivery_id = '{delivery_id}'
        AND message_type = 'delivery.ready.v1' AND status = 'PUBLISHED';""") == attempts
    assert ingestion_sql(f"""SELECT count(*) FROM ingestion_outbox
        WHERE organization_id = '{organization}' AND event_id = '{event_id}'
          AND message_type = 'event.accepted.v1' AND status = 'PUBLISHED';""") == "1"
    wait_for(case + " receiver count", lambda: post(simulator, "/__admin/requests/count",
        {"method": "POST", "url": hook_path}, {})[1]["count"] == int(attempts))
    fact_type = "delivery.succeeded" if retry else "delivery.dead_lettered"
    wait_for(case + " analytics", lambda: clickhouse(f"""
        SELECT count() FROM relayforge_analytics.delivery_facts FINAL
        WHERE organization_id = '{organization}' AND event_id = '{event_id}'
          AND fact_type = 'event.accepted'""") == "1" and clickhouse(f"""
        SELECT count() FROM relayforge_analytics.delivery_facts FINAL
        WHERE organization_id = '{organization}' AND delivery_id = '{delivery_id}'
          AND fact_type = '{fact_type}'""") == "1")
    status, detail = get(delivery_api, "/v1/deliveries/" + delivery_id, bearer)
    assert status == 200 and detail["data"]["delivery"]["status"] == expected
    assert len(detail["data"]["attempts"]) == int(attempts)
    print(f"J2 {case} receiver response reached {expected} in {attempts} attempt(s)")


def publisher_ack_recovery(ingestion, simulator, env, bearer, organization,
                           event_type, hook_path):
    subprocess.run(COMPOSE + ["stop", "ingestion-publisher"], check=True, timeout=30)
    try:
        status, accepted = post_status(ingestion, "/v1/events", {
            "event_type": event_type, "data": {"check": "publisher-recovery"}},
            {**bearer, "Idempotency-Key": str(uuid4())})
        assert status == 202, (status, accepted)
        locator = accepted["data"]["event_id"]
        outbox = wait_for("J2 pending publisher row", lambda: ingestion_sql(f"""
            SELECT id || '|' || event_id FROM ingestion_outbox
            WHERE organization_id = '{organization}' AND partition_key = '{locator}'
              AND message_type = 'event.accepted.v1' AND status = 'PENDING';"""))
        outbox_id, event_id = outbox.split("|")
        publish_duplicate("relayforge.event.accepted.v1", locator,
                          outbox_envelope("ingestion_outbox", outbox_id))
        delivery_id = wait_for("J2 delivery after Kafka ack", lambda: query(f"""
            SELECT delivery.id FROM deliveries delivery JOIN routing_progress progress
              ON progress.accepted_day = delivery.accepted_day
             AND progress.event_id = delivery.event_id
             AND progress.organization_id = delivery.organization_id
            WHERE progress.event_locator = '{locator}'
              AND delivery.organization_id = '{organization}';"""))
        delivery_id = str(UUID(delivery_id))
        assert ingestion_sql(f"""SELECT status FROM ingestion_outbox
            WHERE id = '{outbox_id}';""") == "PENDING"
    finally:
        subprocess.run(COMPOSE + ["start", "ingestion-publisher"], check=True, timeout=30)
    wait_for("J2 publisher replay", lambda: ingestion_sql(f"""
        SELECT status FROM ingestion_outbox WHERE id = '{outbox_id}';""") == "PUBLISHED")
    wait_for("J2 publisher duplicate consumption", lambda: consumer_caught_up(
        f"relayforge-router-events-{env['DELIVERY_SHARD_ID']}",
        "relayforge.event.accepted.v1"))
    assert query(f"""SELECT count(*) FROM deliveries WHERE organization_id = '{organization}'
        AND event_id = '{event_id}';""") == "1"
    assert query(f"""SELECT count(*) FROM delivery_outbox WHERE delivery_id = '{delivery_id}'
        AND message_type = 'delivery.ready.v1';""") == "1"
    wait_for("J2 publisher delivery", lambda: query(f"""SELECT status FROM deliveries
        WHERE id = '{delivery_id}';""") == "SUCCEEDED")
    assert query(f"""SELECT count(*) FROM delivery_attempts WHERE delivery_id = '{delivery_id}'
        AND outcome = 'SUCCEEDED';""") == "1"
    status, received = post(simulator, "/__admin/requests/find", {
        "method": "POST", "url": hook_path,
        "bodyPatterns": [{"contains": delivery_id}]}, {})
    assert status == 200 and len(received["requests"]) == 1
    wait_for("J2 publisher analytics", lambda: clickhouse(f"""
        SELECT count() FROM relayforge_analytics.delivery_facts FINAL
        WHERE organization_id = '{organization}' AND event_id = '{event_id}'
          AND fact_type = 'event.accepted'""") == "1" and clickhouse(f"""
        SELECT count() FROM relayforge_analytics.delivery_facts FINAL
        WHERE organization_id = '{organization}' AND delivery_id = '{delivery_id}'
          AND fact_type = 'delivery.succeeded'""") == "1")
    print("J2 publisher replay after Kafka ack kept one delivery and one receiver call")


def worker_crash_during_send(control, ingestion, simulator, env, bearer, organization,
                             run_id):
    event_type = "acceptance.j2.crash." + run_id
    hook_path = "/j2-crash-" + run_id
    scenario = "j2-worker-crash-" + run_id
    for state, next_state, response in (
            ("Started", "sent", {"status": 204, "fixedDelayMilliseconds": 30000}),
            ("sent", None, {"status": 204})):
        mapping = {"request": {"method": "POST", "urlPath": hook_path},
                   "response": response, "scenarioName": scenario,
                   "requiredScenarioState": state}
        if next_state:
            mapping["newScenarioState"] = next_state
        status, created = post(simulator, "/__admin/mappings", mapping, {})
        assert status == 201 and created["id"]
    status, endpoint = post(control, "/v1/endpoints", {
        "name": "J2 crash receiver " + run_id,
        "destination": {"base_url": env["WEBHOOK_SIMULATOR_URL"], "path": hook_path,
                        "query_params": {}, "headers": {}, "metadata": {}},
        "authentication": {"type": "NONE"}, "event_types": [event_type],
        "delivery_policy": {"timeout_ms": 45000, "max_attempts": 3,
                            "max_concurrency": 2}}, bearer)
    assert status == 201
    endpoint_id = str(UUID(endpoint["data"]["endpoint_id"]))
    wait_for("J2 crash endpoint projection", lambda: query(f"""
        SELECT count(*) FROM subscription_projection WHERE organization_id = '{organization}'
          AND endpoint_id = '{endpoint_id}' AND event_type = '{event_type}'
          AND status = 'ACTIVE';""") == "1")
    status, accepted = post_status(ingestion, "/v1/events", {
        "event_type": event_type, "data": {"check": "crash"}},
        {**bearer, "Idempotency-Key": str(uuid4())})
    assert status == 202, (status, accepted)
    locator = accepted["data"]["event_id"]

    def receiver_started():
        status, scenarios = get(simulator, "/__admin/scenarios", {})
        assert status == 200
        return any(item["name"] == scenario and item["state"] == "sent"
                   for item in scenarios["scenarios"])

    wait_for("J2 receiver accepted first request", receiver_started)
    delivery_id = query(f"""SELECT delivery.id FROM deliveries delivery
        JOIN routing_progress progress ON progress.accepted_day = delivery.accepted_day
         AND progress.event_id = delivery.event_id
         AND progress.organization_id = delivery.organization_id
        WHERE progress.event_locator = '{locator}'
          AND delivery.organization_id = '{organization}';""")
    delivery_id = str(UUID(delivery_id))
    assert query(f"""SELECT outcome FROM delivery_attempts
        WHERE delivery_id = '{delivery_id}' AND attempt_number = 1;""") == "IN_PROGRESS"
    subprocess.run(COMPOSE + ["kill", "-s", "SIGKILL", "delivery-worker"],
                   check=True, timeout=30)
    subprocess.run(COMPOSE + ["start", "delivery-worker"], check=True, timeout=30)
    wait_for("J2 crash recovery", lambda: query(f"""
        SELECT status FROM deliveries WHERE id = '{delivery_id}';""") == "SUCCEEDED", 180)
    assert query(f"""SELECT string_agg(attempt_number || ':' || outcome, ','
        ORDER BY attempt_number) FROM delivery_attempts
        WHERE delivery_id = '{delivery_id}';""") == "1:UNKNOWN,2:SUCCEEDED"
    status, received = post(simulator, "/__admin/requests/find", {
        "method": "POST", "url": hook_path,
        "bodyPatterns": [{"contains": delivery_id}]}, {})
    assert status == 200 and len(received["requests"]) == 2
    seen = sorted(json.loads(base64.b64decode(item["bodyAsBase64"]))["attempt_number"]
                  for item in received["requests"])
    assert seen == [1, 2]
    assert query(f"""SELECT count(*) FROM deliveries WHERE id = '{delivery_id}'
        AND organization_id = '{organization}';""") == "1"
    assert query(f"""SELECT count(*) FROM delivery_outbox WHERE delivery_id = '{delivery_id}'
        AND message_type = 'delivery.ready.v1' AND status = 'PUBLISHED';""") == "2"
    event_id = query(f"""SELECT event_id FROM deliveries WHERE id = '{delivery_id}';""")
    assert ingestion_sql(f"""SELECT count(*) FROM ingestion_outbox WHERE event_id = '{event_id}'
        AND message_type = 'event.accepted.v1' AND status = 'PUBLISHED';""") == "1"
    wait_for("J2 crash analytics", lambda: clickhouse(f"""
        SELECT count() FROM relayforge_analytics.delivery_facts FINAL
        WHERE organization_id = '{organization}' AND event_id = '{event_id}'
          AND fact_type = 'event.accepted'""") == "1" and clickhouse(f"""
        SELECT count() FROM relayforge_analytics.delivery_facts FINAL
        WHERE organization_id = '{organization}' AND delivery_id = '{delivery_id}'
          AND fact_type = 'delivery.succeeded'""") == "1")
    status, event_view = get(ingestion, "/v1/events/" + locator, bearer)
    assert status == 200 and event_view["data"]["delivery_summary"]["succeeded_count"] == 1
    print("J2 worker crash during receiver send recovered one logical delivery")


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
        status, before_send = post(simulator, "/__admin/requests/count",
                                   {"method": "POST", "url": hook_path}, {})
        assert status == 200 and before_send["count"] == 0
        assert query(f"""SELECT count(*) FROM delivery_attempts
            WHERE delivery_id = '{delivery_id}';""") == "0"
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
    publisher_ack_recovery(ingestion, simulator, env, bearer, organization,
                           event_type, hook_path)

    for case, response, first_status, retry in (
            ("400", {"status": 400}, 400, False),
            ("408", {"status": 408}, 408, True),
            ("429", {"status": 429, "headers": {"Retry-After": "1"}}, 429, True),
            ("500", {"status": 500}, 500, True),
            ("timeout", {"status": 200, "fixedDelayMilliseconds": 10000}, None, True),
            ("reset", {"fault": "CONNECTION_RESET_BY_PEER"}, None, True)):
        receiver_case(control, ingestion, delivery_api, simulator, env, bearer, organization,
                      run_id, case, response, first_status, retry)
    worker_crash_during_send(control, ingestion, simulator, env, bearer, organization, run_id)


if __name__ == "__main__":
    main()

"""Verify ClickHouse fact ingestion and recovery from an analytics outage."""

import json
import subprocess
import time
from uuid import UUID, uuid4

from verify_support import post, query, settings


def clickhouse(sql):
    result = subprocess.run(["docker", "compose", "exec", "-T", "clickhouse", "sh", "-c",
                             'clickhouse-client --user "$CLICKHOUSE_USER" '
                             '--password "$CLICKHOUSE_PASSWORD" --query "$1"', "sh", sql],
                            check=True, capture_output=True, text=True, timeout=15)
    return result.stdout.strip()


def wait_fact(organization, fact):
    for _ in range(90):
        try:
            found = clickhouse("SELECT count() FROM relayforge_analytics.delivery_facts FINAL "
                               f"WHERE organization_id = '{UUID(organization)}' "
                               f"AND fact_id = '{UUID(fact)}'")
            if found == "1":
                return
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            pass
        time.sleep(1)
    raise AssertionError("Analytics fact did not reach ClickHouse")


def wait_delivery(locator):
    for _ in range(90):
        row = query("""
            SELECT delivery.id || '|' || delivery.status
            FROM deliveries delivery JOIN routing_progress progress
              ON progress.accepted_day = delivery.accepted_day
             AND progress.event_id = delivery.event_id
             AND progress.organization_id = delivery.organization_id
            WHERE progress.event_locator = '%s';
        """ % locator)
        if row and row.split("|")[1] in ("SUCCEEDED", "DEAD_LETTERED"):
            return row.split("|")
        time.sleep(1)
    raise AssertionError("Delivery did not finish during ClickHouse outage")


def main():
    recent = query("""
        SELECT organization_id || '|' || (payload ->> 'fact_id')
        FROM delivery_outbox WHERE message_type = 'analytics.delivery-facts.v1'
          AND payload ->> 'fact_type' = 'delivery.succeeded'
        ORDER BY created_at DESC LIMIT 1;
    """)
    assert recent, "No successful delivery fact was produced"
    organization, fact = recent.split("|")
    wait_fact(organization, fact)
    print("H2 delivery fact reached ClickHouse")

    envelope = query("""
        SELECT jsonb_build_object('message_id', 'msg_' || id::text,
            'message_type', message_type, 'schema_version', 1,
            'occurred_at', created_at, 'organization_id', organization_id,
            'aggregate_id', payload ->> 'fact_id',
            'correlation_id', 'msg_' || id::text, 'payload', payload)::text
        FROM delivery_outbox WHERE payload ->> 'fact_id' = '%s'
        ORDER BY created_at DESC LIMIT 1;
    """ % UUID(fact))
    assert envelope
    subprocess.run(["docker", "compose", "exec", "-T", "kafka",
                    "/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server",
                    "kafka:9092", "--topic", "relayforge.analytics.delivery-facts.v1",
                    "--property", "parse.key=true", "--property", "key.separator=|"],
                   input=fact + "|" + json.dumps(json.loads(envelope)) + "\n",
                   text=True, capture_output=True, check=True, timeout=20)
    wait_fact(organization, fact)
    print("H2 duplicate fact has one FINAL row")

    env = settings()
    control = f"http://127.0.0.1:{env['CONTROL_API_HOST_PORT']}"
    ingestion = f"http://127.0.0.1:{env['INGESTION_API_HOST_PORT']}"
    simulator = f"http://127.0.0.1:{env['WEBHOOK_SIMULATOR_HOST_PORT']}"
    simulator_url = env["WEBHOOK_SIMULATOR_URL"]
    admin = {"X-Provisioning-Credential": env["TRUSTED_PROVISIONING_CREDENTIAL"],
             "Idempotency-Key": str(uuid4())}
    status, owner = post(control, "/v1/organizations", {"name": "H2 outage"}, admin)
    assert status == 201
    initial = owner["data"]["api_key"]
    status, rotated = post(control, "/v1/api-keys/" + initial.split("_")[2] + "/rotate",
                           {"scopes": ["events:write", "endpoints:write"]}, admin)
    assert status == 201
    bearer = {"Authorization": "Bearer " + rotated["data"]["api_key"]}
    organization = owner["data"]["organization_id"]
    event_type = "acceptance.h2." + uuid4().hex
    hook_path = "/h2-hook-" + uuid4().hex
    status, mapped = post(simulator, "/__admin/mappings", {
        "request": {"method": "POST", "urlPath": hook_path},
        "response": {"status": 204}}, {})
    assert status == 201, mapped
    status, endpoint = post(control, "/v1/endpoints", {
        "name": "H2 outage endpoint",
        "destination": {"base_url": simulator_url, "path": hook_path,
                        "query_params": {}, "headers": {}, "metadata": {}},
        "authentication": {"type": "NONE"}, "event_types": [event_type],
        "delivery_policy": {"timeout_ms": 3000, "max_attempts": 3,
                            "max_concurrency": 2}}, bearer)
    assert status == 201, (status, endpoint)
    endpoint_id = endpoint["data"]["endpoint_id"]
    for _ in range(60):
        if query("SELECT count(*) FROM subscription_projection WHERE endpoint_id = '%s';"
                 % UUID(endpoint_id)) == "1":
            break
        time.sleep(1)
    else:
        raise AssertionError("H2 endpoint did not project")

    subprocess.run(["docker", "compose", "stop", "clickhouse"], check=True)
    try:
        status, accepted = post(ingestion, "/v1/events", {
            "event_type": event_type, "data": {"check": str(uuid4())}},
            {**bearer, "Idempotency-Key": str(uuid4())})
        assert status == 202
        delivery_id, delivery_status = wait_delivery(accepted["data"]["event_id"])
        assert delivery_status == "SUCCEEDED", delivery_status
        pending_fact = query("SELECT payload ->> 'fact_id' FROM delivery_outbox "
                             "WHERE delivery_id = '%s' AND payload ->> 'fact_type' "
                             "= 'delivery.succeeded' LIMIT 1;" % UUID(delivery_id))
        assert pending_fact
    finally:
        subprocess.run(["docker", "compose", "start", "clickhouse"], check=True)
    wait_fact(organization, pending_fact)
    print("H2 delivery continued during ClickHouse outage and analytics caught up")


if __name__ == "__main__":
    main()

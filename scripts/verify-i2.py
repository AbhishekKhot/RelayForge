"""Exercise tenant boundaries and credential canaries on the Compose stack."""

import json
import subprocess
import time
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import uuid4

from verify_support import get, post, post_status, query, settings


def request(base, path, method="GET", body=None, headers=None):
    encoded = None if body is None else json.dumps(body).encode()
    prepared = Request(base + path, encoded, headers or {}, method=method)
    try:
        response = urlopen(prepared, timeout=15)
    except HTTPError as error:
        response = error
    with response:
        payload = response.read()
        return response.status, json.loads(payload) if payload else {}, response.headers


def database(name, sql):
    assert name in {"relayforge_control", "relayforge_delivery"}
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "sh", "-c",
         'psql -qAt -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$1"', "sh", name],
        input=sql + "\n", text=True, capture_output=True, check=True, timeout=20)
    return result.stdout.strip()


def clickhouse(sql):
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "clickhouse", "sh", "-c",
         'clickhouse-client --user "$CLICKHOUSE_USER" '
         '--password "$CLICKHOUSE_PASSWORD" --query "$1"', "sh", sql],
        text=True, capture_output=True, check=True, timeout=45)
    return result.stdout.strip()


def account(control, credential, label):
    admin = {"X-Provisioning-Credential": credential, "Idempotency-Key": str(uuid4())}
    status, created = post(control, "/v1/organizations", {"name": label}, admin)
    assert status == 201, (status, created)
    initial = created["data"]["api_key"]
    scopes = ["events:read", "events:write", "endpoints:read", "endpoints:write",
              "deliveries:read", "deliveries:replay", "analytics:read"]
    status, rotated = post(control, "/v1/api-keys/" + initial.split("_")[2] + "/rotate",
                           {"scopes": scopes}, admin)
    assert status == 201, (status, rotated)
    return created["data"]["organization_id"], rotated["data"]["api_key"]


def verify_public_surface(services, canary):
    for base in services:
        status, live, headers = request(base, "/health/live", headers={
            "Origin": "https://untrusted.example"})
        assert status == 200 and live == {"status": "UP"}, (base, status, live)
        assert "Access-Control-Allow-Origin" not in headers, base
        status, ready, _ = request(base, "/health/ready")
        assert status == 200 and ready == {"status": "UP"}, (base, status, ready)
        for path in ("/actuator/env", "/actuator/metrics", "/actuator/health"):
            status, body, headers = request(base, path, headers={
                "Origin": "https://untrusted.example"})
            assert status in {401, 403, 404}, (base, path, status, body)
            assert "Access-Control-Allow-Origin" not in headers, (base, path)
            assert canary not in json.dumps(body), (base, path)


def main():
    env = settings()
    control = f"http://127.0.0.1:{env['CONTROL_API_HOST_PORT']}"
    ingestion = f"http://127.0.0.1:{env['INGESTION_API_HOST_PORT']}"
    delivery_api = f"http://127.0.0.1:{env['DELIVERY_API_HOST_PORT']}"
    analytics = f"http://127.0.0.1:{env['ANALYTICS_API_HOST_PORT']}"
    simulator = f"http://127.0.0.1:{env['WEBHOOK_SIMULATOR_HOST_PORT']}"
    canary = "I2_PRIVATE_" + uuid4().hex
    owner_id, owner_key = account(control, env["TRUSTED_PROVISIONING_CREDENTIAL"], "I2 owner")
    other_id, other_key = account(control, env["TRUSTED_PROVISIONING_CREDENTIAL"], "I2 other")
    owner = {"Authorization": "Bearer " + owner_key}
    other = {"Authorization": "Bearer " + other_key}
    event_type = "acceptance.i2." + uuid4().hex
    path = "/i2-" + uuid4().hex
    status, _ = post(simulator, "/__admin/mappings", {
        "request": {"method": "POST", "urlPath": path},
        "response": {"status": 400, "body": canary},
    }, {})
    assert status == 201
    endpoint_request = {
        "name": "I2 canary endpoint",
        "destination": {"base_url": env["WEBHOOK_SIMULATOR_URL"], "path": path,
                        "query_params": {}, "headers": {}, "metadata": {}},
        "authentication": {"type": "STATIC_HEADER", "header_name": "X-Api-Key",
                           "value": canary},
        "event_types": [event_type],
        "delivery_policy": {"timeout_ms": 3000, "max_attempts": 1,
                            "max_concurrency": 1},
    }
    status, created = post(control, "/v1/endpoints", endpoint_request, owner)
    assert status == 201, (status, created)
    blocked = {**endpoint_request, "destination": {
        **endpoint_request["destination"], "base_url": "https://169.254.169.254"}}
    status, rejected = post_status(control, "/v1/endpoints", blocked, owner)
    assert status == 422 and canary not in json.dumps(rejected), (status, rejected)
    endpoint_id = created["data"]["endpoint_id"]
    signing_secret = created["data"]["signing_secret"]
    assert canary not in json.dumps(created)
    status, endpoint = get(control, "/v1/endpoints/" + endpoint_id, owner)
    assert status == 200 and canary not in json.dumps(endpoint)
    for path_to_read in ("/v1/endpoints/" + endpoint_id, "/v1/endpoints",
                         "/v1/subscriptions"):
        status, body = get(control, path_to_read, other)
        assert status == (404 if path_to_read.endswith(endpoint_id) else 200), (status, body)
        assert endpoint_id not in json.dumps(body) and canary not in json.dumps(body)
    status, body, _ = request(control, "/v1/endpoints/" + endpoint_id, "PATCH",
                              {"name": "cross-tenant change"},
                              {**other, "Content-Type": "application/json", "If-Match": '"1"'})
    assert status == 404 and canary not in json.dumps(body), (status, body)
    status, body = post_status(control, "/v1/endpoints/" + endpoint_id + "/rotate-secret",
                               {}, {**other, "If-Match": '"1"'})
    assert status == 404 and canary not in json.dumps(body), (status, body)

    key_id = owner_key.split("_")[2]
    stored = database("relayforge_control", "SELECT encode(secret_hash, 'hex') FROM api_keys "
                      f"WHERE key_id = '{key_id}'")
    assert len(stored) == 64 and owner_key not in stored
    encrypted = database("relayforge_control", "SELECT encode(auth_config_ciphertext, 'hex') "
                         f"FROM endpoints WHERE id = '{endpoint_id}' AND organization_id = '{owner_id}'")
    assert encrypted and canary not in encrypted and signing_secret not in encrypted
    assert database("relayforge_control", "SELECT count(*) FROM endpoints "
                    f"WHERE organization_id = '{other_id}' AND id = '{endpoint_id}'") == "0"

    for _ in range(45):
        if query("SELECT count(*) FROM subscription_projection "
                 f"WHERE organization_id = '{owner_id}' AND endpoint_id = '{endpoint_id}' "
                 f"AND event_type = '{event_type}'") == "1":
            break
        time.sleep(1)
    else:
        raise AssertionError("I2 endpoint projection did not arrive")
    status, event = post(ingestion, "/v1/events", {
        "event_type": event_type, "data": {"check": "i2"}},
        {**owner, "Idempotency-Key": str(uuid4())})
    assert status == 202, (status, event)
    event_id = event["data"]["event_id"]
    assert get(ingestion, "/v1/events/" + event_id, other)[0] == 404
    status, owned = get(ingestion, "/v1/events/" + event_id, owner)
    assert status == 200 and owned["data"]["event_id"] == event_id
    delivery_id = None
    for _ in range(75):
        delivery_id = query("SELECT delivery.id FROM deliveries delivery JOIN routing_progress progress "
                            "ON progress.accepted_day = delivery.accepted_day "
                            "AND progress.event_id = delivery.event_id "
                            "AND progress.organization_id = delivery.organization_id "
                            f"WHERE progress.event_locator = '{event_id}' "
                            "AND delivery.status = 'DEAD_LETTERED'")
        if delivery_id:
            break
        time.sleep(1)
    else:
        raise AssertionError("I2 canary delivery did not finish")
    status, detail = get(delivery_api, "/v1/deliveries/" + delivery_id, owner)
    assert status == 200 and canary not in json.dumps(detail), (status, detail)
    assert database("relayforge_delivery", "SELECT count(*) FROM delivery_attempts "
                    f"WHERE delivery_id = '{delivery_id}' AND response_excerpt LIKE '%{canary}%'") == "0"
    assert get(delivery_api, "/v1/deliveries/" + delivery_id, other)[0] == 404
    status, replay = post_status(delivery_api, "/v1/deliveries/" + delivery_id + "/replay",
                                 {"reason": "I2 isolation"},
                                 {**other, "Idempotency-Key": str(uuid4())})
    assert status in {403, 404} and canary not in json.dumps(replay), (status, replay)

    now = datetime.now(timezone.utc)
    window = urlencode({"from": (now - timedelta(days=1)).isoformat(
        timespec="milliseconds").replace("+00:00", "Z"),
        "to": (now + timedelta(minutes=1)).isoformat(
            timespec="milliseconds").replace("+00:00", "Z")})
    for bearer in (owner, other):
        for endpoint in ("/v1/analytics/overview?", "/v1/analytics/deliveries/timeseries?"
                         "interval=hour&"):
            status, body = get(analytics, endpoint + window, bearer)
            assert status == 200 and canary not in json.dumps(body), (status, body)
    for _ in range(45):
        facts = clickhouse("SELECT count() FROM relayforge_analytics.delivery_facts "
                           f"WHERE delivery_id = toUUID('{delivery_id}')")
        if int(facts) > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("I2 analytics fact did not arrive")
    assert clickhouse("SELECT count() FROM relayforge_analytics.delivery_facts "
                      f"WHERE position(fact_type, '{canary}') > 0 "
                      f"OR position(status, '{canary}') > 0 "
                      f"OR position(error_code, '{canary}') > 0") == "0"

    for base in (control, ingestion, delivery_api, analytics):
        verify_public_surface((base,), canary)
        _, _, headers = request(base, "/v1/events", "OPTIONS", headers={
            "Origin": "https://untrusted.example",
            "Access-Control-Request-Method": "POST"})
        assert "Access-Control-Allow-Origin" not in headers, base
    for service in ("control-plane", "ingestion", "delivery-api", "delivery-worker",
                    "analytics-api", "analytics-consumer", "router", "retry-scheduler"):
        logs = subprocess.run(["docker", "compose", "logs", "--no-color", service],
                              text=True, capture_output=True, check=True, timeout=30).stdout
        assert canary not in logs and owner_key not in logs and signing_secret not in logs, service
    print("I2 tenant isolation, encrypted credentials, canary redaction, CORS, and health checks passed")


if __name__ == "__main__":
    main()

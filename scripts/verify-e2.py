"""Verify one signed webhook delivery through the Compose stack."""

import base64
import hashlib
import hmac
import json
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import uuid4


def settings():
    return dict(line.split("=", 1) for line in Path(".env").read_text().splitlines()
                if line and not line.startswith("#") and "=" in line)


def post(base, path, body, headers):
    request = Request(base + path, json.dumps(body).encode(),
                      {"Content-Type": "application/json", **headers}, method="POST")
    with urlopen(request, timeout=10) as response:
        return response.status, json.load(response)


def get(base, path, headers):
    request = Request(base + path, headers=headers, method="GET")
    try:
        with urlopen(request, timeout=10) as response:
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


def query(statement):
    result = subprocess.run(["docker", "compose", "exec", "-T", "postgres", "sh", "-c",
                             'psql -qAt -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d relayforge_delivery'],
                            input=statement + "\n", text=True, capture_output=True,
                            check=True, timeout=20)
    return result.stdout.strip()


def main():
    env = settings()
    control = f"http://127.0.0.1:{env['CONTROL_API_HOST_PORT']}"
    ingestion = f"http://127.0.0.1:{env['INGESTION_API_HOST_PORT']}"
    delivery_api = f"http://127.0.0.1:{env['DELIVERY_API_HOST_PORT']}"
    simulator = f"http://127.0.0.1:{env['WEBHOOK_SIMULATOR_HOST_PORT']}"
    admin = {"X-Provisioning-Credential": env["TRUSTED_PROVISIONING_CREDENTIAL"],
             "Idempotency-Key": str(uuid4())}
    status, owner = post(control, "/v1/organizations", {"name": "E2 delivery"}, admin)
    assert status == 201
    initial_key = owner["data"]["api_key"]
    key_id = initial_key.split("_")[2]
    status, rotated = post(control, f"/v1/api-keys/{key_id}/rotate",
                           {"scopes": ["events:write", "endpoints:write",
                                       "deliveries:read"]}, admin)
    assert status == 201
    bearer = {"Authorization": "Bearer " + rotated["data"]["api_key"]}
    status, endpoint = post(control, "/v1/endpoints", {
        "name": "E2 signed endpoint",
        "destination": {"base_url": env["WEBHOOK_SIMULATOR_URL"],
                        "path": "/status/204", "query_params": {},
                        "headers": {}, "metadata": {}},
        "authentication": {"type": "NONE"},
        "event_types": ["acceptance.e2"],
        "delivery_policy": {"timeout_ms": 3000, "max_attempts": 3,
                            "max_concurrency": 2},
    }, bearer)
    assert status == 201
    secret = endpoint["data"]["signing_secret"]
    status, accepted = post(ingestion, "/v1/events",
                            {"event_type": "acceptance.e2", "data": {"check": str(uuid4())}},
                            {**bearer, "Idempotency-Key": str(uuid4())})
    assert status == 202
    locator = accepted["data"]["event_id"]
    assert locator.startswith("evt_")
    delivery_id = None
    for _ in range(60):
        row = query("""
            SELECT delivery.id || '|' || delivery.status || '|' || delivery.attempt_count
            FROM deliveries delivery JOIN routing_progress progress
              ON progress.accepted_day = delivery.accepted_day
             AND progress.event_id = delivery.event_id
             AND progress.organization_id = delivery.organization_id
            WHERE progress.event_locator = '%s';
        """ % locator)
        if row:
            delivery_id, outcome, count = row.split("|")
            if outcome == "SUCCEEDED":
                assert count == "1", row
                break
        time.sleep(1)
    else:
        raise AssertionError(f"E2 delivery did not succeed: {delivery_id}")
    status, journal = post(simulator, "/__admin/requests/find", {
        "method": "POST", "url": "/status/204",
        "bodyPatterns": [{"contains": delivery_id}],
    }, {})
    assert status == 200 and len(journal["requests"]) == 1
    received = journal["requests"][0]
    body = base64.b64decode(received["bodyAsBase64"])
    headers = {name.lower(): value if isinstance(value, str)
               else value[0] if isinstance(value, list) else value["values"][0]
               for name, value in received["headers"].items()}
    timestamp = headers["x-relayforge-timestamp"]
    signing_key = base64.urlsafe_b64decode(secret + "=" * (-len(secret) % 4))
    expected = hmac.new(signing_key, timestamp.encode() + b"." + body,
                        hashlib.sha256).hexdigest()
    assert headers["x-relayforge-signature"] == "v1=" + expected
    assert headers["x-relayforge-delivery-id"] == delivery_id
    assert json.loads(body)["event_id"] == locator
    print("E2 Compose signed webhook and durable success checks passed")

    now = datetime.now(timezone.utc)
    filters = urlencode({"status": "SUCCEEDED", "endpoint_id": endpoint["data"]["endpoint_id"],
                         "from": (now - timedelta(days=1)).isoformat().replace("+00:00", "Z"),
                         "to": (now + timedelta(seconds=30)).isoformat().replace("+00:00", "Z"),
                         "limit": 1})
    status, page = get(delivery_api, "/v1/deliveries?" + filters, bearer)
    assert status == 200 and len(page["data"]) == 1, (status, page)
    assert page["data"][0]["delivery_id"] == delivery_id
    assert page["next_cursor"] is None
    status, detail = get(delivery_api, "/v1/deliveries/" + delivery_id, bearer)
    assert status == 200 and detail["data"]["delivery"]["status"] == "SUCCEEDED"
    assert detail["data"]["attempts"][0]["http_status"] == 204
    assert detail["data"]["attempts"][0]["attempt_number"] == 1

    other_admin = {**admin, "Idempotency-Key": str(uuid4())}
    status, other = post(control, "/v1/organizations", {"name": "E3 other tenant"}, other_admin)
    assert status == 201
    other_key = other["data"]["api_key"]
    status, other_read = post(control, f"/v1/api-keys/{other_key.split('_')[2]}/rotate",
                              {"scopes": ["deliveries:read"]}, other_admin)
    assert status == 201
    other_bearer = {"Authorization": "Bearer " + other_read["data"]["api_key"]}
    status, _ = get(delivery_api, "/v1/deliveries/" + delivery_id, other_bearer)
    assert status == 404
    print("E3 Compose delivery list, attempt detail, and tenant isolation checks passed")

    for path, response_status, response_headers in (
            ("/f1-throttled", 429, {"Retry-After": "120"}),
            ("/f1-terminal", 400, {})):
        status, _ = post(simulator, "/__admin/mappings", {
            "request": {"method": "POST", "urlPath": path},
            "response": {"status": response_status, "headers": response_headers},
        }, {})
        assert status == 201
        event_type = "acceptance." + path[1:].replace("-", ".")
        status, retry_endpoint = post(control, "/v1/endpoints", {
            "name": "F1 " + path,
            "destination": {"base_url": env["WEBHOOK_SIMULATOR_URL"],
                            "path": path, "query_params": {},
                            "headers": {}, "metadata": {}},
            "authentication": {"type": "NONE"},
            "event_types": [event_type],
            "delivery_policy": {"timeout_ms": 3000, "max_attempts": 3,
                                "max_concurrency": 2},
        }, bearer)
        assert status == 201, (status, retry_endpoint)
        for _ in range(60):
            projected = query("""
                SELECT count(*) FROM subscription_projection
                WHERE organization_id = '%s' AND endpoint_id = '%s'
                  AND event_type = '%s' AND status = 'ACTIVE';
            """ % (owner["data"]["organization_id"],
                   retry_endpoint["data"]["endpoint_id"], event_type))
            if projected == "1":
                break
            time.sleep(1)
        else:
            raise AssertionError("F1 endpoint subscription was not projected")
        status, retry_event = post(ingestion, "/v1/events", {
            "event_type": event_type, "data": {"check": str(uuid4())},
        }, {**bearer, "Idempotency-Key": str(uuid4())})
        assert status == 202
        locator = retry_event["data"]["event_id"]
        expected = "RETRY_SCHEDULED" if response_status == 429 else "DEAD_LETTERED"
        for _ in range(60):
            row = query("""
                SELECT delivery.status || '|' || delivery.attempt_count || '|'
                    || COALESCE(delivery.last_http_status::text, '') || '|'
                    || (SELECT count(*) FROM delivery_work work
                        WHERE work.delivery_id = delivery.id)
                FROM deliveries delivery JOIN routing_progress progress
                  ON progress.accepted_day = delivery.accepted_day
                 AND progress.event_id = delivery.event_id
                 AND progress.organization_id = delivery.organization_id
                WHERE progress.event_locator = '%s';
            """ % locator)
            if row and row.split("|")[0] == expected:
                break
            time.sleep(1)
        else:
            raise AssertionError(f"F1 delivery did not reach {expected}: {row}")
        outcome, count, http_status, work_count = row.split("|")
        assert count == "1" and http_status == str(response_status), row
        assert work_count == ("1" if response_status == 429 else "0"), row
        status, detail = get(delivery_api, "/v1/deliveries/" +
                             query("""
                                 SELECT delivery.id FROM deliveries delivery
                                 JOIN routing_progress progress
                                   ON progress.accepted_day = delivery.accepted_day
                                  AND progress.event_id = delivery.event_id
                                  AND progress.organization_id = delivery.organization_id
                                 WHERE progress.event_locator = '%s';
                             """ % locator), bearer)
        assert status == 200 and detail["data"]["attempts"][0]["http_status"] == response_status
        if response_status == 429:
            remaining = query("""
                SELECT EXTRACT(EPOCH FROM (work.next_attempt_at - clock_timestamp()))::int
                FROM delivery_work work JOIN routing_progress progress
                  ON progress.accepted_day = work.accepted_day
                 AND progress.organization_id = work.organization_id
                JOIN deliveries delivery
                  ON delivery.accepted_day = work.accepted_day AND delivery.id = work.delivery_id
                 AND delivery.event_id = progress.event_id
                WHERE progress.event_locator = '%s';
            """ % locator)
            assert int(remaining) >= 60, remaining
    print("F1 Compose bounded Retry-After and terminal response checks passed")


if __name__ == "__main__":
    main()

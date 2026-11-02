"""Verify one signed webhook delivery through the Compose stack."""

import base64
import hashlib
import hmac
import json
import subprocess
import time
from pathlib import Path
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
    simulator = f"http://127.0.0.1:{env['WEBHOOK_SIMULATOR_HOST_PORT']}"
    admin = {"X-Provisioning-Credential": env["TRUSTED_PROVISIONING_CREDENTIAL"],
             "Idempotency-Key": str(uuid4())}
    status, owner = post(control, "/v1/organizations", {"name": "E2 delivery"}, admin)
    assert status == 201
    initial_key = owner["data"]["api_key"]
    key_id = initial_key.split("_")[2]
    status, rotated = post(control, f"/v1/api-keys/{key_id}/rotate",
                           {"scopes": ["events:write", "endpoints:write"]}, admin)
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


if __name__ == "__main__":
    main()

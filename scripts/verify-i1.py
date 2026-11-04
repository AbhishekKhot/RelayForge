"""Verify quota enforcement, immediate revision refresh, and fail-closed database access."""

import json
import subprocess
import time
from urllib.request import Request, urlopen
from uuid import uuid4

from verify_support import get, post, post_status, settings


def replace_quotas(control, organization, credential, overrides):
    request = Request(control + "/v1/organizations/" + organization + "/quotas",
                      json.dumps({"quota_overrides": overrides}).encode(),
                      {"Content-Type": "application/json",
                       "X-Provisioning-Credential": credential}, method="PUT")
    with urlopen(request, timeout=10) as response:
        return response.status, json.load(response)


def endpoint(control, bearer, destination, name):
    return post_status(control, "/v1/endpoints", {
        "name": name,
        "destination": {"base_url": destination, "path": "/i1-hook",
                        "query_params": {}, "headers": {}, "metadata": {}},
        "authentication": {"type": "NONE"},
        "event_types": ["acceptance.i1." + uuid4().hex],
        "delivery_policy": {"timeout_ms": 3000, "max_attempts": 2,
                            "max_concurrency": 2}}, bearer)


def event(ingestion, bearer, key, data):
    return post_status(ingestion, "/v1/events", {
        "event_type": "acceptance.i1.no_subscribers", "data": data},
        {**bearer, "Idempotency-Key": key})


def main():
    env = settings()
    control = f"http://127.0.0.1:{env['CONTROL_API_HOST_PORT']}"
    ingestion = f"http://127.0.0.1:{env['INGESTION_API_HOST_PORT']}"
    admin = {"X-Provisioning-Credential": env["TRUSTED_PROVISIONING_CREDENTIAL"],
             "Idempotency-Key": str(uuid4())}
    status, created = post(control, "/v1/organizations", {
        "name": "I1 quotas",
        "quota_overrides": {"events_per_minute": 1, "burst_events": 1,
                            "max_active_endpoints": 1,
                            "max_event_payload_bytes": 1024}}, admin)
    assert status == 201, created
    organization = created["data"]["organization_id"]
    first_key = created["data"]["api_key"]
    status, rotated = post(control, "/v1/api-keys/" + first_key.split("_")[2] + "/rotate",
                           {"scopes": ["events:write", "endpoints:write"]}, admin)
    assert status == 201, rotated
    bearer = {"Authorization": "Bearer " + rotated["data"]["api_key"]}
    assert get(control, "/v1/auth/check", bearer)[1]["quotas"]["burst_events"] == 1

    status, oversized = event(ingestion, bearer, str(uuid4()), {"value": "x" * 2000})
    assert status == 413 and oversized["code"] == "PAYLOAD_TOO_LARGE", oversized
    key = str(uuid4())
    status, accepted = event(ingestion, bearer, key, {"value": "small"})
    assert status == 202, accepted
    status, duplicate = event(ingestion, bearer, key, {"value": "small"})
    assert status == 202 and duplicate["data"]["event_id"] == accepted["data"]["event_id"]
    status, limited = event(ingestion, bearer, str(uuid4()), {"value": "another"})
    assert status == 429 and limited["code"] == "QUOTA_EXCEEDED", limited

    destination = env["WEBHOOK_SIMULATOR_URL"]
    assert endpoint(control, bearer, destination, "I1 endpoint one")[0] == 201
    status, limited = endpoint(control, bearer, destination, "I1 endpoint two")
    assert status == 429 and limited["code"] == "QUOTA_EXCEEDED", limited
    status, updated = replace_quotas(control, organization,
                                     env["TRUSTED_PROVISIONING_CREDENTIAL"],
                                     {"events_per_minute": 1, "burst_events": 1,
                                      "max_active_endpoints": 2,
                                      "max_event_payload_bytes": 1024})
    assert status == 200 and updated["data"]["quotas"]["max_active_endpoints"] == 2
    assert get(control, "/v1/auth/check", bearer)[1]["quotas"]["max_active_endpoints"] == 2
    assert endpoint(control, bearer, destination, "I1 endpoint two")[0] == 201
    print("I1 event burst, payload, endpoint, idempotency, and cache refresh checks passed")

    subprocess.run(["docker", "compose", "stop", "postgres"], check=True)
    try:
        status, unavailable = get(control, "/v1/auth/check", bearer)
        assert status == 503, unavailable
        status, unavailable = event(ingestion, bearer, str(uuid4()), {"value": "offline"})
        assert status == 503, unavailable
    finally:
        subprocess.run(["docker", "compose", "start", "postgres"], check=True)
    for _ in range(30):
        status, _ = get(control, "/v1/auth/check", bearer)
        if status == 200:
            break
        time.sleep(1)
    else:
        raise AssertionError("PostgreSQL auth lookup did not recover")
    print("I1 PostgreSQL outage failed closed and recovered")


if __name__ == "__main__":
    main()

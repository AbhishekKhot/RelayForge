"""Verify token reuse, version separation, and bounded Redis outage fallback."""

import subprocess
import time
from uuid import uuid4

from verify_g1 import mapping
from verify_support import post, query, settings


def wait_delivery(locator):
    for _ in range(90):
        row = query("""
            SELECT delivery.status FROM deliveries delivery
            JOIN routing_progress progress ON progress.accepted_day = delivery.accepted_day
              AND progress.event_id = delivery.event_id
              AND progress.organization_id = delivery.organization_id
            WHERE progress.event_locator = '%s';
        """ % locator)
        if row in ("SUCCEEDED", "DEAD_LETTERED"):
            return row
        time.sleep(1)
    raise AssertionError("G2 delivery did not finish: " + locator)


def main():
    env = settings()
    control = f"http://127.0.0.1:{env['CONTROL_API_HOST_PORT']}"
    ingestion = f"http://127.0.0.1:{env['INGESTION_API_HOST_PORT']}"
    simulator = f"http://127.0.0.1:{env['WEBHOOK_SIMULATOR_HOST_PORT']}"
    simulator_url = env["WEBHOOK_SIMULATOR_URL"]
    admin = {"X-Provisioning-Credential": env["TRUSTED_PROVISIONING_CREDENTIAL"],
             "Idempotency-Key": str(uuid4())}
    status, owner = post(control, "/v1/organizations", {"name": "G2 cache"}, admin)
    assert status == 201
    initial = owner["data"]["api_key"]
    status, rotated = post(control, "/v1/api-keys/" + initial.split("_")[2] + "/rotate",
                           {"scopes": ["events:write", "endpoints:write"]}, admin)
    assert status == 201
    bearer = {"Authorization": "Bearer " + rotated["data"]["api_key"]}
    token_path = "/g2-token-" + uuid4().hex
    hook_path = "/g2-hook-" + uuid4().hex
    mapping(simulator, token_path, {"status": 200,
            "headers": {"Content-Type": "application/json"},
            "jsonBody": {"access_token": "g2-shared", "expires_in": 120}})
    mapping(simulator, hook_path, {"status": 204}, authorization="Bearer g2-shared")
    event_type = "acceptance.g2." + uuid4().hex
    auth = {"type": "CLIENT_CREDENTIALS", "token_url": simulator_url + token_path,
            "client_id": "g2-client", "client_secret": "g2-secret",
            "token_response": {"access_token_path": "$.access_token",
                               "expires_in_path": "$.expires_in", "token_type": "Bearer"}}
    for index in range(2):
        status, endpoint = post(control, "/v1/endpoints", {
            "name": "G2 endpoint " + str(index),
            "destination": {"base_url": simulator_url, "path": hook_path,
                            "query_params": {}, "headers": {}, "metadata": {}},
            "authentication": auth, "event_types": [event_type + str(index)],
            "delivery_policy": {"timeout_ms": 3000, "max_attempts": 3,
                                "max_concurrency": 8}}, bearer)
        assert status == 201, (status, endpoint)
        endpoint_id = endpoint["data"]["endpoint_id"]
        for _ in range(60):
            if query("SELECT count(*) FROM subscription_projection WHERE endpoint_id = '%s';"
                     % endpoint_id) == "1":
                break
            time.sleep(1)
        else:
            raise AssertionError("G2 endpoint not projected")
        locators = []
        for _ in range(5 if index == 0 else 1):
            status, accepted = post(ingestion, "/v1/events", {
                "event_type": event_type + str(index), "data": {"check": str(uuid4())}},
                {**bearer, "Idempotency-Key": str(uuid4())})
            assert status == 202
            locators.append(accepted["data"]["event_id"])
        for locator in locators:
            assert wait_delivery(locator) == "SUCCEEDED"
        status, calls = post(simulator, "/__admin/requests/find",
                             {"method": "POST", "url": token_path}, {})
        assert status == 200 and len(calls["requests"]) == index + 1

    subprocess.run(["docker", "compose", "stop", "redis"], check=True)
    try:
        status, endpoint = post(control, "/v1/endpoints", {
            "name": "G2 outage endpoint",
            "destination": {"base_url": simulator_url, "path": hook_path,
                            "query_params": {}, "headers": {}, "metadata": {}},
            "authentication": auth, "event_types": [event_type + "outage"],
            "delivery_policy": {"timeout_ms": 3000, "max_attempts": 3,
                                "max_concurrency": 2}}, bearer)
        assert status == 201
        endpoint_id = endpoint["data"]["endpoint_id"]
        for _ in range(60):
            if query("SELECT count(*) FROM subscription_projection WHERE endpoint_id = '%s';"
                     % endpoint_id) == "1":
                break
            time.sleep(1)
        else:
            raise AssertionError("G2 outage endpoint not projected")
        started = time.monotonic()
        status, accepted = post(ingestion, "/v1/events", {
            "event_type": event_type + "outage", "data": {"check": str(uuid4())}},
            {**bearer, "Idempotency-Key": str(uuid4())})
        assert status == 202
        assert wait_delivery(accepted["data"]["event_id"]) == "SUCCEEDED"
        assert time.monotonic() - started < 90
    finally:
        subprocess.run(["docker", "compose", "start", "redis"], check=True)
    print("G2 token reuse, endpoint isolation, and Redis outage fallback passed")


if __name__ == "__main__":
    main()

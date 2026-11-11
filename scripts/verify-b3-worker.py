"""Verify queued deliveries use the latest endpoint configuration."""

import json
import subprocess
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import UUID, uuid4

from verify_support import post, query, settings


COMPOSE = ["docker", "compose", "-f", "compose.yaml",
           "-f", "deploy/ingestion-ha/compose.yaml"]


def wait_for(label, probe, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        found = probe()
        if found:
            return found
        time.sleep(1)
    raise AssertionError("Timed out waiting for " + label)


def patch(base, path, bearer, body, version):
    request = Request(base + path, json.dumps(body).encode(), {
        "Authorization": bearer["Authorization"], "Content-Type": "application/json",
        "If-Match": '"' + str(version) + '"'}, method="PATCH")
    try:
        with urlopen(request, timeout=10) as response:
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


def worker(action):
    result = subprocess.run(COMPOSE + [action, "delivery-worker"], text=True,
                            capture_output=True, timeout=45)
    assert result.returncode == 0, result.stderr.strip()


def endpoint(control, bearer, simulator_url, event_type, path):
    status, created = post(control, "/v1/endpoints", {
        "name": "B3 worker " + uuid4().hex,
        "destination": {"base_url": simulator_url, "path": path,
                        "query_params": {}, "headers": {}, "metadata": {}},
        "authentication": {"type": "NONE"}, "event_types": [event_type],
        "delivery_policy": {"timeout_ms": 3000, "max_attempts": 2,
                            "max_concurrency": 2}}, bearer)
    assert status == 201, (status, created)
    return str(UUID(created["data"]["endpoint_id"]))


def publish(ingestion, bearer, event_type):
    status, accepted = post(ingestion, "/v1/events", {
        "event_type": event_type, "data": {"check": uuid4().hex}},
        {**bearer, "Idempotency-Key": str(uuid4())})
    assert status == 202, (status, accepted)
    return accepted["data"]["event_id"]


def delivery(locator):
    row = query("""
        SELECT delivery.id || '|' || delivery.status || '|' || delivery.attempt_count
          || '|' || COALESCE(delivery.last_error_code, '')
        FROM deliveries delivery JOIN routing_progress progress
          ON progress.accepted_day = delivery.accepted_day
         AND progress.event_id = delivery.event_id
         AND progress.organization_id = delivery.organization_id
        WHERE progress.event_locator = '%s';
    """ % locator)
    return row.split("|") if row else None


def request_count(simulator, path):
    status, found = post(simulator, "/__admin/requests/count",
                         {"method": "POST", "url": path}, {})
    assert status == 200, (status, found)
    return found["count"]


def mapping(simulator, path):
    status, created = post(simulator, "/__admin/mappings", {
        "request": {"method": "POST", "urlPath": path},
        "response": {"status": 204}}, {})
    assert status == 201, (status, created)


def main():
    env = settings()
    control = f"http://127.0.0.1:{env['CONTROL_API_HOST_PORT']}"
    ingestion = f"http://127.0.0.1:{env['INGESTION_API_HOST_PORT']}"
    simulator = f"http://127.0.0.1:{env['WEBHOOK_SIMULATOR_HOST_PORT']}"
    simulator_url = env["WEBHOOK_SIMULATOR_URL"]
    admin = {"X-Provisioning-Credential": env["TRUSTED_PROVISIONING_CREDENTIAL"],
             "Idempotency-Key": str(uuid4())}
    status, owner = post(control, "/v1/organizations", {"name": "B3 worker freshness"}, admin)
    assert status == 201, (status, owner)
    initial = owner["data"]["api_key"]
    status, rotated = post(control, "/v1/api-keys/" + initial.split("_")[2] + "/rotate",
                           {"scopes": ["events:write", "endpoints:write"]}, admin)
    assert status == 201, (status, rotated)
    bearer = {"Authorization": "Bearer " + rotated["data"]["api_key"]}
    organization = str(UUID(owner["data"]["organization_id"]))

    for scenario in ("updated", "disabled"):
        run_id = uuid4().hex
        event_type = "acceptance.b3.worker." + scenario + "." + run_id
        old_path = "/b3-old-" + run_id
        new_path = "/b3-new-" + run_id
        mapping(simulator, old_path)
        mapping(simulator, new_path)
        endpoint_id = endpoint(control, bearer, simulator_url, event_type, old_path)
        wait_for("B3 endpoint projection", lambda: query("""
            SELECT count(*) FROM subscription_projection WHERE organization_id = '%s'
              AND endpoint_id = '%s' AND event_type = '%s' AND status = 'ACTIVE';
        """ % (organization, endpoint_id, event_type)) == "1")
        worker("stop")
        try:
            locator = publish(ingestion, bearer, event_type)
            wait_for("B3 queued delivery", lambda: delivery(locator))
            if scenario == "updated":
                body = {"destination": {"base_url": simulator_url, "path": new_path,
                                        "query_params": {}, "headers": {}, "metadata": {}}}
            else:
                body = {"status": "DISABLED"}
            status, changed = patch(control, "/v1/endpoints/" + endpoint_id,
                                    bearer, body, 1)
            assert status == 200 and changed["data"]["version"] == 2, (status, changed)
        finally:
            worker("start")
        expected = "SUCCEEDED" if scenario == "updated" else "CANCELLED"
        row = wait_for("B3 " + scenario + " delivery", lambda: (
            result if (result := delivery(locator)) and result[1] == expected else None))
        assert row[2] == ("1" if scenario == "updated" else "0"), row
        assert row[3] == ("" if scenario == "updated" else "ENDPOINT_DISABLED"), row
        assert request_count(simulator, old_path) == 0
        assert request_count(simulator, new_path) == (1 if scenario == "updated" else 0)
        assert query("SELECT count(*) FROM delivery_work WHERE delivery_id = '%s';"
                     % UUID(row[0])) == "0"
        print("B3 queued delivery " + scenario + " against current endpoint configuration")


if __name__ == "__main__":
    main()

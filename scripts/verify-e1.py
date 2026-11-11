"""Verify the platform claim cap and lease renewal across slow authentication and HTTP."""

import time
from uuid import UUID, uuid4

from verify_support import post, query, settings


def wait_for(label, probe, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = probe()
        if result:
            return result
        time.sleep(1)
    raise AssertionError("Timed out waiting for " + label)


def mapping(simulator, path, response, **scenario):
    body = {"request": {"method": "POST", "urlPath": path}, "response": response}
    body.update(scenario)
    status, created = post(simulator, "/__admin/mappings", body, {})
    assert status == 201, (status, created)


def endpoint(control, bearer, simulator_url, event_type, path, authentication,
             timeout_ms=30000):
    status, created = post(control, "/v1/endpoints", {
        "name": "E1 " + uuid4().hex,
        "destination": {"base_url": simulator_url, "path": path,
                        "query_params": {}, "headers": {}, "metadata": {}},
        "authentication": authentication,
        "event_types": [event_type],
        "delivery_policy": {"timeout_ms": timeout_ms, "max_attempts": 2,
                            "max_concurrency": 8}}, bearer)
    assert status == 201, (status, created)
    return str(UUID(created["data"]["endpoint_id"]))


def publish(ingestion, bearer, event_type):
    status, accepted = post(ingestion, "/v1/events", {
        "event_type": event_type, "data": {"check": uuid4().hex}},
        {**bearer, "Idempotency-Key": str(uuid4())})
    assert status == 202, (status, accepted)
    return accepted["data"]["event_id"]


def deliveries(locator):
    rows = query("""
        SELECT delivery.id || '|' || delivery.status || '|' || delivery.attempt_count
            || '|' || delivery.claim_count || '|' || delivery.version
            || '|' || COALESCE(delivery.lease_token::text, '')
        FROM deliveries delivery JOIN routing_progress progress
          ON progress.accepted_day = delivery.accepted_day
         AND progress.event_id = delivery.event_id
         AND progress.organization_id = delivery.organization_id
        WHERE progress.event_locator = '%s' ORDER BY delivery.id;
    """ % locator)
    return [row.split("|") for row in rows.splitlines() if row]


def platform_cap(control, ingestion, simulator, simulator_url, bearer, organization, limit):
    run_id = uuid4().hex
    event_type = "acceptance.e1.cap." + run_id
    path = "/e1-cap-" + run_id
    mapping(simulator, path, {"status": 204, "fixedDelayMilliseconds": 12000})
    endpoint_ids = [endpoint(control, bearer, simulator_url, event_type, path,
                             {"type": "NONE"}) for _ in range(limit + 2)]
    wait_for("E1 projected endpoints", lambda: query("""
        SELECT count(*) FROM subscription_projection
        WHERE organization_id = '%s' AND event_type = '%s' AND status = 'ACTIVE';
    """ % (organization, event_type)) == str(len(endpoint_ids)))
    locator = publish(ingestion, bearer, event_type)
    maximum_active = 0
    deferred = False
    deadline = time.monotonic() + 150
    while time.monotonic() < deadline:
        rows = deliveries(locator)
        if len(rows) == len(endpoint_ids):
            active = sum(row[1] == "IN_PROGRESS" for row in rows)
            maximum_active = max(maximum_active, active)
            assert active <= limit, (active, limit, rows)
            deferred |= any(row[1] in ("PENDING", "ENQUEUED", "RETRY_SCHEDULED")
                            for row in rows) and active == limit
            if all(row[1] == "SUCCEEDED" for row in rows):
                assert maximum_active == limit, maximum_active
                assert deferred, "Cap did not defer competing work"
                assert all(row[2] == "1" and row[3] == "1" for row in rows), rows
                print(f"E1 platform cap held at {maximum_active}; all {len(rows)} deliveries succeeded")
                return
        time.sleep(1)
    raise AssertionError("E1 capped deliveries did not finish")


def long_lease(control, ingestion, simulator, simulator_url, bearer, organization):
    run_id = uuid4().hex
    event_type = "acceptance.e1.renew." + run_id
    token_path = "/e1-token-" + run_id
    hook_path = "/e1-hook-" + run_id
    scenario = "e1-auth-" + run_id
    states = ["Started", "failure-1", "failure-2", "failure-3", "ready"]
    for index in range(4):
        mapping(simulator, token_path, {"status": 500, "fixedDelayMilliseconds": 9000},
                scenarioName=scenario, requiredScenarioState=states[index],
                newScenarioState=states[index + 1])
    mapping(simulator, token_path, {"status": 200, "fixedDelayMilliseconds": 9000,
                                    "headers": {"Content-Type": "application/json"},
                                    "jsonBody": {"access_token": "e1-lease-token"}},
            scenarioName=scenario, requiredScenarioState="ready")
    mapping(simulator, hook_path, {"status": 204, "fixedDelayMilliseconds": 15000})
    authentication = {
        "type": "CLIENT_CREDENTIALS", "token_url": simulator_url + token_path,
        "client_id": "e1-client", "client_secret": "e1-secret",
        "token_response": {"access_token_path": "$.access_token", "token_type": "Bearer"}}
    endpoint_id = endpoint(control, bearer, simulator_url, event_type, hook_path,
                           authentication)
    wait_for("E1 renewal endpoint projection", lambda: query("""
        SELECT count(*) FROM subscription_projection WHERE organization_id = '%s'
          AND endpoint_id = '%s' AND event_type = '%s' AND status = 'ACTIVE';
    """ % (organization, endpoint_id, event_type)) == "1")
    locator = publish(ingestion, bearer, event_type)
    versions = set()
    tokens = set()
    started = time.monotonic()
    deadline = started + 180
    while time.monotonic() < deadline:
        rows = deliveries(locator)
        if rows:
            assert len(rows) == 1, rows
            row = rows[0]
            if row[1] == "IN_PROGRESS":
                versions.add(int(row[4]))
                tokens.add(row[5])
            if row[1] == "SUCCEEDED":
                assert len(versions) >= 3, versions
                assert len(tokens) == 1 and "" not in tokens, tokens
                assert row[2:4] == ["1", "1"], row
                assert time.monotonic() - started >= 60
                delivery_id = str(UUID(row[0]))
                assert query("SELECT count(*) FROM authentication_attempts "
                             "WHERE delivery_id = '%s';" % delivery_id) == "5"
                assert query("SELECT count(*) FROM delivery_attempts "
                             "WHERE delivery_id = '%s' AND outcome = 'SUCCEEDED';"
                             % delivery_id) == "1"
                print("E1 renewed one lease across long token exchange and slow webhook")
                return
            assert row[1] != "DEAD_LETTERED", row
        time.sleep(1)
    raise AssertionError("E1 long token and webhook delivery did not finish")


def main():
    env = settings()
    limit = int(env.get("DELIVERY_PLATFORM_CONCURRENCY_LIMIT", "4"))
    assert limit == 4, "E1 acceptance expects the local four-worker platform cap"
    control = f"http://127.0.0.1:{env['CONTROL_API_HOST_PORT']}"
    ingestion = f"http://127.0.0.1:{env['INGESTION_API_HOST_PORT']}"
    simulator = f"http://127.0.0.1:{env['WEBHOOK_SIMULATOR_HOST_PORT']}"
    admin = {"X-Provisioning-Credential": env["TRUSTED_PROVISIONING_CREDENTIAL"],
             "Idempotency-Key": str(uuid4())}
    status, owner = post(control, "/v1/organizations", {"name": "E1 lease acceptance"}, admin)
    assert status == 201, (status, owner)
    initial = owner["data"]["api_key"]
    status, rotated = post(control, "/v1/api-keys/" + initial.split("_")[2] + "/rotate",
                           {"scopes": ["events:write", "endpoints:write"]}, admin)
    assert status == 201, (status, rotated)
    bearer = {"Authorization": "Bearer " + rotated["data"]["api_key"]}
    organization = str(UUID(owner["data"]["organization_id"]))
    platform_cap(control, ingestion, simulator, env["WEBHOOK_SIMULATOR_URL"],
                 bearer, organization, limit)
    long_lease(control, ingestion, simulator, env["WEBHOOK_SIMULATOR_URL"],
               bearer, organization)


if __name__ == "__main__":
    main()

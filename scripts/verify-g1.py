"""Verify dynamic token authentication through the Compose stack."""

import base64
import json
import subprocess
import time
from uuid import uuid4

from verify_support import post, query, settings


def mapping(simulator, path, response, *, scenario=None, required=None, next_state=None,
            authorization=None):
    body = {"request": {"method": "POST", "urlPath": path}, "response": response}
    if authorization:
        body["request"]["headers"] = {"Authorization": {"equalTo": authorization}}
    if scenario:
        body["scenarioName"] = scenario
        body["requiredScenarioState"] = required
        if next_state:
            body["newScenarioState"] = next_state
    status, result = post(simulator, "/__admin/mappings", body, {})
    assert status == 201, (status, result)


def delivery(control, ingestion, bearer, owner, simulator_url, suffix, authentication):
    event_type = "acceptance.g1." + suffix
    status, endpoint = post(control, "/v1/endpoints", {
        "name": "G1 " + suffix,
        "destination": {"base_url": simulator_url, "path": "/g1-" + suffix + "-hook",
                        "query_params": {}, "headers": {}, "metadata": {}},
        "authentication": authentication,
        "event_types": [event_type],
        "delivery_policy": {"timeout_ms": 3000, "max_attempts": 3, "max_concurrency": 2},
    }, bearer)
    assert status == 201, (status, endpoint)
    endpoint_id = endpoint["data"]["endpoint_id"]
    for _ in range(60):
        if query("SELECT count(*) FROM subscription_projection WHERE organization_id = '%s' "
                 "AND endpoint_id = '%s' AND event_type = '%s';" %
                 (owner, endpoint_id, event_type)) == "1":
            break
        time.sleep(1)
    else:
        raise AssertionError("G1 endpoint was not projected")
    status, accepted = post(ingestion, "/v1/events", {
        "event_type": event_type, "data": {"check": str(uuid4())},
    }, {**bearer, "Idempotency-Key": str(uuid4())})
    assert status == 202, (status, accepted)
    locator = accepted["data"]["event_id"]
    for _ in range(90):
        row = query("""
            SELECT delivery.id || '|' || delivery.status || '|' || delivery.attempt_count
            FROM deliveries delivery JOIN routing_progress progress
              ON progress.accepted_day = delivery.accepted_day
             AND progress.event_id = delivery.event_id
             AND progress.organization_id = delivery.organization_id
            WHERE progress.event_locator = '%s';
        """ % locator)
        if row and row.split("|")[1] in ("SUCCEEDED", "DEAD_LETTERED"):
            return row.split("|")
        time.sleep(1)
    raise AssertionError("G1 delivery did not finish: " + row)


def main():
    env = settings()
    control = f"http://127.0.0.1:{env['CONTROL_API_HOST_PORT']}"
    ingestion = f"http://127.0.0.1:{env['INGESTION_API_HOST_PORT']}"
    simulator = f"http://127.0.0.1:{env['WEBHOOK_SIMULATOR_HOST_PORT']}"
    simulator_url = env["WEBHOOK_SIMULATOR_URL"]
    admin = {"X-Provisioning-Credential": env["TRUSTED_PROVISIONING_CREDENTIAL"],
             "Idempotency-Key": str(uuid4())}
    status, owner = post(control, "/v1/organizations", {"name": "G1 dynamic auth"}, admin)
    assert status == 201
    initial = owner["data"]["api_key"]
    status, rotated = post(control, "/v1/api-keys/" + initial.split("_")[2] + "/rotate",
                           {"scopes": ["events:write", "endpoints:write"]}, admin)
    assert status == 201
    bearer = {"Authorization": "Bearer " + rotated["data"]["api_key"]}
    organization = owner["data"]["organization_id"]

    mapping(simulator, "/g1-oauth-token", {"status": 200,
            "headers": {"Content-Type": "application/json"},
            "jsonBody": {"access_token": "g1-oauth-token", "expires_in": 120}})
    mapping(simulator, "/g1-oauth-hook", {"status": 204},
            authorization="Bearer g1-oauth-token")
    oauth = {"type": "CLIENT_CREDENTIALS", "token_url": simulator_url + "/g1-oauth-token",
             "client_id": "g1-client", "client_secret": "g1-secret",
             "token_response": {"access_token_path": "$.access_token",
                                "expires_in_path": "$.expires_in", "token_type": "Bearer"}}
    oauth_id, oauth_status, oauth_attempts = delivery(control, ingestion, bearer, organization,
                                                       simulator_url, "oauth", oauth)
    assert (oauth_status, oauth_attempts) == ("SUCCEEDED", "1")
    assert query("SELECT count(*) FROM authentication_attempts WHERE delivery_id = '%s' "
                 "AND outcome = 'SUCCEEDED';" % oauth_id) == "1"
    status, found = post(simulator, "/__admin/requests/find", {
        "method": "POST", "url": "/g1-oauth-hook",
        "headers": {"Authorization": {"equalTo": "Bearer g1-oauth-token"}},
    }, {})
    assert status == 200 and len(found["requests"]) == 1
    status, token_calls = post(simulator, "/__admin/requests/find", {
        "method": "POST", "url": "/g1-oauth-token",
    }, {})
    assert status == 200 and len(token_calls["requests"]) == 1
    token_header = token_calls["requests"][0]["headers"]["Authorization"]
    encoded = token_header[0] if isinstance(token_header, list) else token_header
    if isinstance(encoded, dict):
        encoded = encoded["values"][0]
    assert base64.b64decode(encoded.split(" ", 1)[1]) == b"g1-client:g1-secret"
    print("G1 OAuth client credentials token exchange and webhook passed")

    mapping(simulator, "/g1-exchange-token", {"status": 200,
            "headers": {"Content-Type": "application/json"},
            "jsonBody": {"data": {"accessToken": "g1-exchange-token"}}})
    mapping(simulator, "/g1-exchange-hook", {"status": 204},
            authorization="Bearer g1-exchange-token")
    exchange = {"type": "TOKEN_EXCHANGE", "token_request": {
        "method": "POST", "url": simulator_url + "/g1-exchange-token",
        "headers": {"Content-Type": "application/json"},
        "body_fields": {"username": {"source": "secret", "name": "username"},
                        "mode": {"source": "literal", "value": "delivery"}}},
        "token_response": {"access_token_path": "$.data.accessToken", "token_type": "Bearer"},
        "token_placement": {"type": "HEADER", "name": "Authorization", "prefix": "Bearer "},
        "secrets": {"username": "g1-user"}}
    exchange_id, exchange_status, exchange_attempts = delivery(control, ingestion, bearer,
            organization, simulator_url, "exchange", exchange)
    assert (exchange_status, exchange_attempts) == ("SUCCEEDED", "1")
    assert query("SELECT count(*) FROM authentication_attempts WHERE delivery_id = '%s';" %
                 exchange_id) == "1"
    status, exchange_call = post(simulator, "/__admin/requests/find", {
        "method": "POST", "url": "/g1-exchange-token",
        "bodyPatterns": [{"equalToJson": json.dumps({"username": "g1-user", "mode": "delivery"})}],
    }, {})
    assert status == 200 and len(exchange_call["requests"]) == 1
    print("G1 generic token exchange passed")

    mapping(simulator, "/g1-retry-token", {"status": 500},
            scenario="g1-auth-retry", required="Started", next_state="after-500")
    mapping(simulator, "/g1-retry-token", {"status": 429},
            scenario="g1-auth-retry", required="after-500", next_state="ready")
    mapping(simulator, "/g1-retry-token", {"status": 200,
            "headers": {"Content-Type": "application/json"},
            "jsonBody": {"access_token": "g1-retry-token"}},
            scenario="g1-auth-retry", required="ready")
    mapping(simulator, "/g1-retry-hook", {"status": 204},
            authorization="Bearer g1-retry-token")
    retry = {**oauth, "token_url": simulator_url + "/g1-retry-token"}
    retry["token_response"] = {"access_token_path": "$.access_token", "token_type": "Bearer"}
    retry_id, retry_status, retry_attempts = delivery(control, ingestion, bearer, organization,
                                                      simulator_url, "retry", retry)
    assert (retry_status, retry_attempts) == ("SUCCEEDED", "1")
    assert query("SELECT string_agg(outcome, ',' ORDER BY attempt_number) "
                 "FROM authentication_attempts WHERE delivery_id = '%s';" % retry_id) == \
           "RETRYABLE_FAILURE,RETRYABLE_FAILURE,SUCCEEDED"
    print("G1 transient token responses retry without consuming webhook attempts")

    mapping(simulator, "/g1-refresh-token", {"status": 200,
            "headers": {"Content-Type": "application/json"},
            "jsonBody": {"access_token": "g1-stale-token"}},
            scenario="g1-refresh", required="Started", next_state="fresh")
    mapping(simulator, "/g1-refresh-token", {"status": 200,
            "headers": {"Content-Type": "application/json"},
            "jsonBody": {"access_token": "g1-fresh-token"}},
            scenario="g1-refresh", required="fresh")
    mapping(simulator, "/g1-refresh-hook", {"status": 401},
            authorization="Bearer g1-stale-token")
    mapping(simulator, "/g1-refresh-hook", {"status": 204},
            authorization="Bearer g1-fresh-token")
    refresh = {**retry, "token_url": simulator_url + "/g1-refresh-token"}
    refresh_id, refresh_status, refresh_attempts = delivery(control, ingestion, bearer,
            organization, simulator_url, "refresh", refresh)
    assert (refresh_status, refresh_attempts) == ("SUCCEEDED", "1")
    assert query("SELECT count(*) FROM authentication_attempts WHERE delivery_id = '%s' "
                 "AND outcome = 'SUCCEEDED';" % refresh_id) == "2"
    print("G1 webhook 401 refreshes once within one delivery attempt")

    mapping(simulator, "/g1-malformed-token", {"status": 200,
            "headers": {"Content-Type": "application/json"}, "jsonBody": {"unexpected": "value"}})
    mapping(simulator, "/g1-malformed-hook", {"status": 204})
    malformed = {**retry, "token_url": simulator_url + "/g1-malformed-token"}
    invalid_id, invalid_status, invalid_attempts = delivery(control, ingestion, bearer,
            organization, simulator_url, "malformed", malformed)
    assert (invalid_status, invalid_attempts) == ("DEAD_LETTERED", "0")
    assert query("SELECT count(*) FROM delivery_attempts WHERE delivery_id = '%s';" % invalid_id) == "0"
    assert query("SELECT outcome FROM authentication_attempts WHERE delivery_id = '%s';" %
                 invalid_id) == "TERMINAL_FAILURE"
    mapping(simulator, "/g1-invalid-token", {"status": 401})
    mapping(simulator, "/g1-invalid-hook", {"status": 204})
    invalid_credentials = {**retry, "token_url": simulator_url + "/g1-invalid-token"}
    rejected_id, rejected_status, rejected_attempts = delivery(control, ingestion, bearer,
            organization, simulator_url, "invalid", invalid_credentials)
    assert (rejected_status, rejected_attempts) == ("DEAD_LETTERED", "0")
    assert query("SELECT outcome || '|' || http_status FROM authentication_attempts "
                 "WHERE delivery_id = '%s';" % rejected_id) == "TERMINAL_FAILURE|401"
    logs = subprocess.run(["docker", "compose", "logs", "delivery-worker"],
                          text=True, capture_output=True, check=True, timeout=20).stdout
    assert "g1-secret" not in logs and "g1-oauth-token" not in logs and "g1-user" not in logs
    print("G1 malformed token response fails fast without leaking credentials")


if __name__ == "__main__":
    main()

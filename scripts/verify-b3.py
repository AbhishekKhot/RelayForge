"""Exercise endpoint management and tenant isolation on the Compose stack."""

import json
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import uuid4


def settings():
    return dict(line.split("=", 1) for line in Path(".env").read_text().splitlines()
                if line and not line.startswith("#") and "=" in line)


def request(base, method, path, key=None, body=None, headers=None):
    request_headers = dict(headers or {})
    if key:
        request_headers["Authorization"] = "Bearer " + key
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        request_headers["Content-Type"] = "application/json"
    outgoing = Request(base + path, data=data, headers=request_headers, method=method)
    try:
        with urlopen(outgoing, timeout=10) as response:
            return response.status, json.load(response), response.headers
    except HTTPError as error:
        return error.code, json.load(error), error.headers


def provision(base, credential, name):
    headers = {"X-Provisioning-Credential": credential,
               "Idempotency-Key": str(uuid4())}
    status, organization, _ = request(base, "POST", "/v1/organizations",
                                      body={"name": name}, headers=headers)
    assert status == 201
    key_id = organization["data"]["api_key"].split("_")[2]
    status, rotated, _ = request(base, "POST", f"/v1/api-keys/{key_id}/rotate",
                                 body={"scopes": ["endpoints:write", "endpoints:read"]},
                                 headers=headers)
    assert status == 201
    return rotated["data"]["api_key"]


def main():
    env = settings()
    base = f"http://127.0.0.1:{env['CONTROL_API_HOST_PORT']}"
    key = provision(base, env["TRUSTED_PROVISIONING_CREDENTIAL"], "B3 owner")
    other_key = provision(base, env["TRUSTED_PROVISIONING_CREDENTIAL"], "B3 other")
    body = {
        "name": "B3 endpoint",
        "destination": {"base_url": env["WEBHOOK_SIMULATOR_URL"], "path": "/hook",
                        "query_params": {}, "headers": {}, "metadata": {}},
        "authentication": {"type": "NONE"},
        "event_types": ["acceptance.b3.one", "acceptance.b3.two"],
        "delivery_policy": {"timeout_ms": 10000, "max_attempts": 3,
                            "max_concurrency": 2},
    }
    status, created, _ = request(base, "POST", "/v1/endpoints", key, body)
    assert status == 201
    endpoint_id = created["data"]["endpoint_id"]
    status, _, _ = request(base, "POST", "/v1/endpoints", key,
                           {**body, "event_types": ["acceptance.b3.one", "invalid type"]})
    assert status in (400, 422)

    status, endpoints, _ = request(base, "GET", "/v1/endpoints?limit=10", key)
    assert status == 200 and [item["endpoint_id"] for item in endpoints["data"]] == [endpoint_id]
    status, first_page, _ = request(base, "GET", "/v1/subscriptions?limit=1", key)
    assert status == 200 and len(first_page["data"]) == 1 and first_page["next_cursor"]
    cursor = urlencode({"limit": 1, "cursor": first_page["next_cursor"]})
    status, last_page, _ = request(base, "GET", "/v1/subscriptions?" + cursor, key)
    assert status == 200 and len(last_page["data"]) == 1
    assert {first_page["data"][0]["event_type"], last_page["data"][0]["event_type"]} == set(body["event_types"])

    status, _, _ = request(base, "GET", f"/v1/endpoints/{endpoint_id}", other_key)
    assert status == 404
    status, detail, _ = request(base, "GET", f"/v1/endpoints/{endpoint_id}", key)
    assert status == 200 and detail["data"]["version"] == 1
    status, updated, response_headers = request(base, "PATCH", f"/v1/endpoints/{endpoint_id}",
                                                 key, {"status": "DISABLED"},
                                                 {"If-Match": '"1"'})
    assert status == 200 and updated["data"]["status"] == "DISABLED"
    assert updated["data"]["version"] == 2 and response_headers.get("ETag") == '"2"'
    status, _, _ = request(base, "PATCH", f"/v1/endpoints/{endpoint_id}", key,
                           {"name": "stale"}, {"If-Match": '"1"'})
    assert status == 412
    status, rotated, _ = request(base, "POST", f"/v1/endpoints/{endpoint_id}/rotate-secret",
                                 key, {}, {"If-Match": '"2"'})
    assert status == 200 and rotated["data"]["signing_secret"]
    assert rotated["data"]["version"] == 3
    status, detail, _ = request(base, "GET", f"/v1/endpoints/{endpoint_id}", key)
    assert status == 200 and detail["data"]["status"] == "DISABLED"
    assert detail["data"]["version"] == 3 and "signing_secret" not in json.dumps(detail)
    print("B3 Compose endpoint management, pagination, rollback, and tenant checks passed")


if __name__ == "__main__":
    main()

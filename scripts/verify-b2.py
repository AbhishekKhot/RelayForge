"""Exercise redacted endpoint authentication registration on the Compose stack."""

import json
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4


def settings():
    return dict(line.split("=", 1) for line in Path(".env").read_text().splitlines()
                if line and not line.startswith("#") and "=" in line)


def post(base, path, body, headers):
    request = Request(base + path, json.dumps(body).encode(),
                      {"Content-Type": "application/json", **headers}, method="POST")
    try:
        with urlopen(request, timeout=10) as response:
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


def get(base, path, key):
    request = Request(base + path, headers={"Authorization": "Bearer " + key})
    with urlopen(request, timeout=10) as response:
        return response.status, json.load(response)


def main():
    env = settings()
    base = f"http://127.0.0.1:{env['CONTROL_API_HOST_PORT']}"
    admin = {"X-Provisioning-Credential": env["TRUSTED_PROVISIONING_CREDENTIAL"],
             "Idempotency-Key": str(uuid4())}
    status, organization = post(base, "/v1/organizations", {"name": "B2 acceptance"}, admin)
    assert status == 201, status
    key_id = organization["data"]["api_key"].split("_")[2]
    status, rotated = post(base, f"/v1/api-keys/{key_id}/rotate",
                           {"scopes": ["endpoints:write", "endpoints:read"]}, admin)
    assert status == 201, status
    key = rotated["data"]["api_key"]
    bearer = {"Authorization": "Bearer " + key}
    configurations = [
        {"type": "STATIC_HEADER", "header_name": "X-Api-Key", "value": "b2-static-secret"},
        {"type": "STATIC_API_KEY", "header_name": "X-Api-Key", "value": "b2-key-secret"},
        {"type": "BEARER", "token": "b2-bearer-secret"},
        {"type": "BASIC", "username": "customer", "password": "b2-basic-secret"},
    ]
    for configuration in configurations:
        body = {"name": "B2 endpoint", "destination": {
                    "base_url": env["WEBHOOK_SIMULATOR_URL"], "path": "/hook",
                    "query_params": {}, "headers": {}, "metadata": {}},
                "authentication": configuration, "event_types": ["acceptance.b2"],
                "delivery_policy": {"timeout_ms": 10000, "max_attempts": 3, "max_concurrency": 2}}
        status, created = post(base, "/v1/endpoints", body, bearer)
        assert status == 201, (status, created)
        endpoint_id = created["data"]["endpoint_id"]
        status, read = get(base, f"/v1/endpoints/{endpoint_id}", key)
        assert status == 200 and read["data"]["authentication"] == {"type": configuration["type"]}
        assert "secret" not in json.dumps(read).lower()
    print("B2 Compose authentication registration and redaction checks passed")


if __name__ == "__main__":
    main()

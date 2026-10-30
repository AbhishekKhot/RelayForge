"""Exercise endpoint registration against the local Compose stack."""

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


def main():
    env = settings()
    base = f"http://127.0.0.1:{env['CONTROL_API_HOST_PORT']}"
    admin = {"X-Provisioning-Credential": env["TRUSTED_PROVISIONING_CREDENTIAL"],
             "Idempotency-Key": str(uuid4())}
    status, organization = post(base, "/v1/organizations", {"name": "B1 acceptance"}, admin)
    assert status == 201, (status, organization)
    first_key = organization["data"]["api_key"]
    key_id = first_key.split("_")[2]
    status, rotated = post(base, f"/v1/api-keys/{key_id}/rotate",
                           {"scopes": ["endpoints:write"]}, admin)
    assert status == 201, (status, rotated)
    bearer = {"Authorization": "Bearer " + rotated["data"]["api_key"]}
    destination = {"base_url": env["WEBHOOK_SIMULATOR_URL"], "path": "/hook",
                   "query_params": {"acceptance": "b1"}, "headers": {}, "metadata": {}}
    body = {"name": "Acceptance endpoint", "destination": destination,
            "authentication": {"type": "NONE"}, "event_types": ["acceptance.test"],
            "delivery_policy": {"timeout_ms": 10000, "max_attempts": 3, "max_concurrency": 2}}
    status, created = post(base, "/v1/endpoints", body, bearer)
    assert status == 201 and created["data"]["status"] == "ACTIVE", (status, created)
    for blocked in ("https://169.254.169.254", "https://127.0.0.1"):
        body["destination"]["base_url"] = blocked
        status, rejected = post(base, "/v1/endpoints", body, bearer)
        assert status == 422 and blocked not in json.dumps(rejected), (status, rejected)
    print("B1 Compose endpoint registration and SSRF checks passed")


if __name__ == "__main__":
    main()

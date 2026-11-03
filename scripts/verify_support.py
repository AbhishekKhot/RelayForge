"""Shared HTTP and PostgreSQL helpers for Compose acceptance checks."""

import json
import subprocess
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen


def settings():
    return dict(line.split("=", 1) for line in Path(".env").read_text().splitlines()
                if line and not line.startswith("#") and "=" in line)


def post(base, path, body, headers):
    request = Request(base + path, json.dumps(body).encode(),
                      {"Content-Type": "application/json", **headers}, method="POST")
    with urlopen(request, timeout=10) as response:
        return response.status, json.load(response)


def post_status(base, path, body, headers):
    try:
        return post(base, path, body, headers)
    except HTTPError as error:
        return error.code, json.load(error)


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

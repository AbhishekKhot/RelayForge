"""Verify scoped analytics aggregates, validation, freshness, and outage response."""

import subprocess
import time
from urllib.parse import urlencode
from uuid import UUID, uuid4

from verify_support import get, post, settings


def clickhouse(sql):
    command = ["docker", "compose", "exec", "-T", "clickhouse", "sh", "-c",
               'clickhouse-client --user "$CLICKHOUSE_USER" '
               '--password "$CLICKHOUSE_PASSWORD" --query "$1"', "sh", sql]
    for attempt in range(3):
        try:
            subprocess.run(command, check=True, capture_output=True, text=True, timeout=45)
            return
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            if attempt == 2:
                raise
            time.sleep(2)


def owner(control, label, credential):
    admin = {"X-Provisioning-Credential": credential,
             "Idempotency-Key": str(uuid4())}
    status, created = post(control, "/v1/organizations", {"name": label}, admin)
    assert status == 201, created
    initial = created["data"]["api_key"]
    status, rotated = post(control, "/v1/api-keys/" + initial.split("_")[2] + "/rotate",
                           {"scopes": ["analytics:read"]}, admin)
    assert status == 201, rotated
    return created["data"]["organization_id"], {
        "Authorization": "Bearer " + rotated["data"]["api_key"]}


def fact(organization, endpoint, fact_id, version, fact_type, occurred):
    clickhouse("INSERT INTO relayforge_analytics.delivery_facts "
               "(organization_id,event_date,fact_id,version,endpoint_id,fact_type,status,occurred_at) "
               f"VALUES ('{UUID(organization)}','2026-11-03','{UUID(fact_id)}',{version},"
               f"'{UUID(endpoint)}','{fact_type}','SUCCEEDED','{occurred}')")


def main():
    env = settings()
    control = f"http://127.0.0.1:{env['CONTROL_API_HOST_PORT']}"
    analytics = f"http://127.0.0.1:{env['ANALYTICS_API_HOST_PORT']}"
    first, first_key = owner(control, "H3 first tenant", env["TRUSTED_PROVISIONING_CREDENTIAL"])
    second, second_key = owner(control, "H3 second tenant", env["TRUSTED_PROVISIONING_CREDENTIAL"])
    endpoint = str(uuid4())
    correction = str(uuid4())
    fact(first, endpoint, str(uuid4()), 1, "event.accepted", "2026-11-03 10:00:00")
    fact(first, endpoint, str(uuid4()), 1, "delivery.created", "2026-11-03 10:05:00")
    fact(first, endpoint, correction, 1, "delivery.succeeded", "2026-11-03 10:10:00")
    fact(first, endpoint, correction, 2, "delivery.succeeded", "2026-11-03 10:10:00")
    fact(second, endpoint, str(uuid4()), 1, "delivery.dead_lettered", "2026-11-03 10:15:00")
    window = urlencode({"from": "2026-11-03T10:00:00Z", "to": "2026-11-03T11:00:00Z"})
    overview = "/v1/analytics/overview?" + window
    status, first_result = get(analytics, overview, first_key)
    assert status == 200, first_result
    assert first_result["data"] == {
        "events_accepted": 1, "deliveries_created": 1, "deliveries_succeeded": 1,
        "deliveries_dead_lettered": 0, "deliveries_retry_scheduled": 0}, first_result
    assert first_result["data_as_of"] and first_result["request_id"]
    status, second_result = get(analytics, overview, second_key)
    assert status == 200 and second_result["data"]["events_accepted"] == 0
    assert second_result["data"]["deliveries_dead_lettered"] == 1
    series = "/v1/analytics/deliveries/timeseries?" + window + "&interval=hour&endpoint_id=" + endpoint
    status, first_series = get(analytics, series, first_key)
    assert status == 200, first_series
    assert len(first_series["data"]["points"]) == 1, first_series
    assert first_series["data"]["points"][0]["deliveries_succeeded"] == 1
    assert first_series["data"]["points"][0]["bucket"] == "2026-11-03T10:00:00Z"
    status, other_endpoint = get(analytics, series.replace(endpoint, str(uuid4())), first_key)
    assert status == 200 and other_endpoint["data"]["points"] == []
    oversized = urlencode({"from": "2026-09-01T10:00:00Z",
                           "to": "2026-11-03T11:00:00Z"})
    status, invalid = get(analytics, "/v1/analytics/overview?" + oversized, first_key)
    assert status == 400, invalid
    status, invalid = get(analytics, series.replace("interval=hour", "interval=minute"), first_key)
    assert status == 400, invalid
    print("H3 tenant-scoped FINAL totals, timeseries, freshness, and bounds passed")

    subprocess.run(["docker", "compose", "stop", "clickhouse"], check=True)
    try:
        status, unavailable = get(analytics, overview, first_key)
        assert status == 503 and unavailable["code"] == "ANALYTICS_TEMPORARILY_UNAVAILABLE", unavailable
    finally:
        subprocess.run(["docker", "compose", "start", "clickhouse"], check=True)
    print("H3 ClickHouse outage returned bounded analytics 503")


if __name__ == "__main__":
    main()

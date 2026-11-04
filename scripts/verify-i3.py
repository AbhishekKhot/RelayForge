"""Exercise observability endpoints and dependency readiness on the base Compose stack."""

import subprocess
import time


def compose(*args, timeout=60):
    result = subprocess.run(["docker", "compose", *args],
                            text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise AssertionError(f"docker compose {args} failed: {result.stderr}")
    return result.stdout


def probe(service, expected):
    return subprocess.run(["docker", "compose", "exec", "-T", service,
                           "java", "-cp", "/app", "HealthProbe",
                           "http://127.0.0.1:8080/health/ready", str(expected)],
                          text=True, capture_output=True, timeout=20).returncode == 0


def await_probe(service, expected, seconds=75):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if probe(service, expected):
            return
        time.sleep(2)
    raise AssertionError(f"{service} readiness did not become {expected}")


def metric(service, name):
    return compose("run", "--rm", "--no-deps", "--entrypoint", "curl",
                   "test-control-plane", "-fsS",
                   f"http://{service}:9090/actuator/metrics/{name}", timeout=90)


def main():
    for service, names in {
        "control-plane": ("http.server.requests",),
        "control-publisher": ("relayforge.outbox.pending",),
        "ingestion": ("http.server.requests",),
        "ingestion-publisher": ("relayforge.outbox.pending",),
        "router": ("relayforge.kafka.consumer.lag.max", "relayforge.routing.fanout"),
        "delivery-worker": ("relayforge.kafka.consumer.lag.max",
                            "relayforge.delivery.attempts"),
        "retry-scheduler": ("relayforge.delivery.backlog",
                            "relayforge.delivery.retry.due.backlog"),
        "analytics-consumer": ("relayforge.analytics.ingest.lag.seconds",
                               "relayforge.clickhouse.insert"),
        "analytics-api": ("relayforge.analytics.query.duration",),
    }.items():
        await_probe(service, 200, seconds=120)
        for name in names:
            assert name in metric(service, name), (service, name)

    for dependency, affected, fallback in (
        ("postgres", "control-plane", None),
        ("kafka", "router", None),
        ("redis", "ingestion", "control-plane"),
        ("clickhouse", "analytics-api", None),
    ):
        compose("stop", dependency, timeout=90)
        try:
            await_probe(affected, 503)
            if fallback:
                await_probe(fallback, 200)
        finally:
            compose("start", dependency, timeout=90)
        await_probe(affected, 200, seconds=120)
    print("I3 management metrics and dependency readiness checks passed")


if __name__ == "__main__":
    main()

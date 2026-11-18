"""Bounded J3 profile runner and sanitized evidence collector."""

import argparse
import json
import math
import os
import shutil
import subprocess
import time
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


PROFILES = {"normal": 5000, "peak": 10000, "headroom": 15000,
            "controlled": 50}
THRESHOLDS = {"accepted_ratio": 0.99, "accepted_p95_ms": 500,
              "accepted_p99_ms": 1000, "delivery_p95_s": 5,
              "delivery_p99_s": 30, "drain_s": 120}
COMPOSE_FILES = ("compose.yaml", "deploy/ingestion-ha/compose.yaml", "load-tests/compose.yaml")


def quantile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def parse_kafka_lag(output):
    lag = Counter()
    for line in output.splitlines():
        fields = line.split()
        if len(fields) >= 6 and fields[2].isdigit() and fields[5].isdigit():
            lag[fields[1]] += int(fields[5])
    return dict(lag)


def summarize_records(records, scheduled):
    outcomes = Counter(record.get("outcome") for record in records)
    accepted_latencies = [record["latency_ms"] for record in records
                          if record.get("outcome") == "accepted"]
    return {"scheduled": scheduled, "completed": len(records),
            "accepted": outcomes["accepted"], "rejected": outcomes["rejected"],
            "transport_errors": outcomes["error"], "skipped": max(0, scheduled - len(records)),
            "accepted_latency_ms": {"p50": quantile(accepted_latencies, 0.5),
                                    "p95": quantile(accepted_latencies, 0.95),
                                    "p99": quantile(accepted_latencies, 0.99)},
            "rejections": dict(Counter(str(record.get("status")) for record in records
                                       if record.get("outcome") == "rejected")),
            "errors": dict(Counter(record.get("error_code", "OTHER") for record in records
                                   if record.get("outcome") == "error"))}


def evaluate(summary, target_rate, duration_seconds, delivery=None, drain_seconds=None):
    findings = []
    if summary["accepted"] < target_rate * duration_seconds * THRESHOLDS["accepted_ratio"]:
        findings.append("accepted_rate")
    if summary["skipped"]:
        findings.append("generator_skipped")
    for label in ("p95", "p99"):
        value = summary["accepted_latency_ms"].get(label)
        if value is None or value > THRESHOLDS[f"accepted_{label}_ms"]:
            findings.append(f"accepted_latency_{label}")
    if delivery is None:
        findings.append("delivery_lag_unmeasured")
    else:
        for label in ("p95", "p99"):
            value = delivery.get(f"lag_{label}_s")
            if value is None or value > THRESHOLDS[f"delivery_{label}_s"]:
                findings.append(f"delivery_lag_{label}")
    if drain_seconds is None or drain_seconds > THRESHOLDS["drain_s"]:
        findings.append("backlog_drain")
    return {"status": "unmet" if findings else "met", "findings": findings}


def ensure_clean(snapshot_data):
    issues = []
    ingestion = snapshot_data.get("ingestion", {})
    delivery = snapshot_data.get("delivery", {})
    if ingestion.get("events") != 0:
        issues.append("previous events")
    if ingestion.get("outbox_pending") != 0:
        issues.append("ingestion outbox")
    if delivery.get("deliveries") != 0:
        issues.append("previous deliveries")
    if delivery.get("backlog") != 0:
        issues.append("delivery backlog")
    if delivery.get("outbox_pending") != 0:
        issues.append("delivery outbox")
    lag = snapshot_data.get("kafka_lag")
    if not isinstance(lag, dict) or any(value != 0 for value in lag.values()):
        issues.append("Kafka lag")
    return issues


def pipeline_drained(snapshot_data):
    ingestion = snapshot_data.get("ingestion", {})
    delivery = snapshot_data.get("delivery", {})
    lag = snapshot_data.get("kafka_lag")
    events = ingestion.get("events")
    return (isinstance(events, int)
            and delivery.get("routing_events") == events
            and delivery.get("routing_incomplete") == 0
            and isinstance(delivery.get("deliveries"), int)
            and delivery.get("succeeded", 0) + delivery.get("dead_lettered", 0)
                == delivery["deliveries"]
            and ingestion.get("outbox_pending") == 0
            and delivery.get("backlog") == 0
            and delivery.get("outbox_pending") == 0
            and isinstance(lag, dict)
            and all(value == 0 for value in lag.values()))


def observe_drain(initial, sampler, max_seconds, clock=time.monotonic, sleeper=time.sleep):
    started = clock()
    current = initial
    samples = []
    while True:
        if pipeline_drained(current):
            return round(clock() - started, 3), samples, current
        remaining = max_seconds - (clock() - started)
        if remaining <= 0:
            return None, samples, current
        sleeper(min(5, remaining))
        current = sampler()
        samples.append(current)


def command(args, timeout=20):
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"Command failed: {args[0]} exit {result.returncode}")
    return result.stdout.strip()


def compose_args(env_file, project):
    result = ["docker", "compose", "--env-file", str(env_file), "-p", project]
    for path in COMPOSE_FILES:
        result.extend(("-f", path))
    return result


def sql(compose, service, database, query):
    output = command(compose + ["exec", "-T", service, "sh", "-c",
        'PGPASSWORD="$POSTGRES_PASSWORD" psql -qAt -v ON_ERROR_STOP=1 '
        '-U "$POSTGRES_USER" -d "$1" -c "SET statement_timeout = 5000; $2"',
        "sh", database, query], timeout=15)
    return output.splitlines()[-1]


INGESTION_SQL = """SELECT json_build_object(
    'events', (SELECT count(*) FROM events),
    'registry', (SELECT count(*) FROM idempotency_registry),
    'outbox_pending', (SELECT count(*) FROM ingestion_outbox WHERE status <> 'PUBLISHED'),
    'database_bytes', pg_database_size(current_database()),
    'wal_bytes', (SELECT wal_bytes FROM pg_stat_wal),
    'active_backends', (SELECT count(*) FROM pg_stat_activity
        WHERE datname = current_database() AND state = 'active'),
    'lock_waiters', (SELECT count(*) FROM pg_stat_activity
        WHERE datname = current_database() AND wait_event_type = 'Lock'),
    'commits', (SELECT xact_commit FROM pg_stat_database WHERE datname = current_database()),
    'deadlocks', (SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()))"""

DELIVERY_SQL = """SELECT json_build_object(
    'active_subscriptions', (SELECT count(*) FROM subscription_projection WHERE status = 'ACTIVE'),
    'routing_events', (SELECT count(*) FROM routing_progress),
    'routing_incomplete', (SELECT count(*) FROM routing_progress
        WHERE status IN ('SELECTING', 'ROUTING')),
    'deliveries', (SELECT count(*) FROM deliveries),
    'attempts', (SELECT count(*) FROM delivery_attempts),
    'backlog', (SELECT count(*) FROM delivery_work),
    'succeeded', (SELECT count(*) FROM deliveries WHERE status = 'SUCCEEDED'),
    'dead_lettered', (SELECT count(*) FROM deliveries WHERE status = 'DEAD_LETTERED'),
    'outbox_pending', (SELECT count(*) FROM delivery_outbox WHERE status <> 'PUBLISHED'),
    'database_bytes', pg_database_size(current_database()),
    'wal_bytes', (SELECT wal_bytes FROM pg_stat_wal),
    'active_backends', (SELECT count(*) FROM pg_stat_activity
        WHERE datname = current_database() AND state = 'active'),
    'lock_waiters', (SELECT count(*) FROM pg_stat_activity
        WHERE datname = current_database() AND wait_event_type = 'Lock'),
    'commits', (SELECT xact_commit FROM pg_stat_database WHERE datname = current_database()),
    'deadlocks', (SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()))"""

DELIVERY_LAG_SQL = """SELECT json_build_object(
    'completed', count(*),
    'lag_p95_s', percentile_cont(0.95) WITHIN GROUP
        (ORDER BY extract(epoch FROM d.completed_at - r.event_accepted_at)),
    'lag_p99_s', percentile_cont(0.99) WITHIN GROUP
        (ORDER BY extract(epoch FROM d.completed_at - r.event_accepted_at)))
    FROM deliveries d JOIN routing_progress r ON
        d.accepted_day = r.accepted_day AND d.event_id = r.event_id
        AND d.organization_id = r.organization_id
    WHERE d.completed_at IS NOT NULL AND r.event_accepted_at >= TIMESTAMPTZ '%s'"""


def safe_probe(probe):
    try:
        return probe()
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
        return {"probe_error": type(error).__name__}


def snapshot(compose, settings):
    def database(service, name, query):
        return json.loads(sql(compose, service, name, query))

    def kafka():
        output = command(compose + ["exec", "-T", "kafka",
            "/opt/kafka/bin/kafka-consumer-groups.sh", "--bootstrap-server", "kafka:9092",
            "--all-groups", "--describe"], timeout=20)
        return parse_kafka_lag(output)

    def redis():
        output = command(compose + ["exec", "-T", "redis", "sh", "-c",
            'REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli INFO'], timeout=15)
        wanted = {"keyspace_hits", "keyspace_misses", "used_memory", "connected_clients",
                  "instantaneous_ops_per_sec"}
        return {key: int(value) for line in output.splitlines() if ":" in line
                for key, value in [line.strip().split(":", 1)]
                if key in wanted and value.isdigit()}

    def clickhouse():
        query = ("SELECT coalesce(sum(rows), 0) FROM system.parts WHERE database = "
                 "'relayforge_analytics' AND table = 'delivery_facts' AND active")
        output = command(compose + ["exec", "-T", "clickhouse", "sh", "-c",
            'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" '
            '--query "$1"', "sh", query], timeout=15)
        return {"active_part_rows": int(output)}

    def clickhouse_query():
        query = ("SELECT count(), quantileExact(0.95)(query_duration_ms) "
                 "FROM system.query_log WHERE type = 'QueryFinish' AND "
                 "query_kind = 'Select' AND event_time > now() - INTERVAL 5 MINUTE")
        output = command(compose + ["exec", "-T", "clickhouse", "sh", "-c",
            'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" '
            '--query "$1"', "sh", query], timeout=15)
        count, p95 = output.split("\t")
        return {"recent_selects": int(count), "recent_select_p95_ms": float(p95)}

    def docker_stats():
        container_ids = command(compose + ["ps", "-q"], timeout=15).splitlines()
        if not container_ids:
            return {}
        output = command(["docker", "stats", "--no-stream", "--format", "{{json .}}",
                          *container_ids], timeout=20)
        return {item["Name"]: {key: item[key] for key in ("CPUPerc", "MemPerc", "MemUsage", "PIDs")}
                for line in output.splitlines() for item in [json.loads(line)]}

    def pool(service):
        output = command(compose + ["run", "--rm", "--no-deps", "--entrypoint", "curl",
            "test-control-plane", "-fsS",
            f"http://{service}:9090/actuator/metrics/hikaricp.connections.active"], timeout=30)
        measurements = json.loads(output)["measurements"]
        return {item["statistic"]: item["value"] for item in measurements}

    def receiver():
        port = settings.get("LOAD_RECEIVER_HOST_PORT", "18085")
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/__stats", timeout=5) as response:
            return json.load(response)

    return {"time_utc": datetime.now(timezone.utc).isoformat(),
            "ingestion": safe_probe(lambda: database("ingestion-primary", "relayforge_ingestion", INGESTION_SQL)),
            "delivery": safe_probe(lambda: database("postgres", "relayforge_delivery", DELIVERY_SQL)),
            "kafka_lag": safe_probe(kafka), "redis": safe_probe(redis),
            "clickhouse": safe_probe(clickhouse),
            "clickhouse_queries": safe_probe(clickhouse_query),
            "docker_stats": safe_probe(docker_stats),
            "ingestion_pool": safe_probe(lambda: pool("ingestion")),
            "delivery_pool": safe_probe(lambda: pool("delivery-worker")),
            "receiver": safe_probe(receiver)}


def settings_file(path):
    return dict(line.split("=", 1) for line in Path(path).read_text().splitlines()
                if line and not line.startswith("#") and "=" in line)


def run_profile(args):
    rate = PROFILES[args.profile]
    env_file = Path(args.env_file).resolve()
    settings = settings_file(env_file)
    compose = compose_args(env_file, args.project)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    run_id = uuid4().hex[:12]
    raw = output_dir / f"{args.profile}-{run_id}.ndjson"
    before = snapshot(compose, settings)
    dirty = ensure_clean(before)
    if dirty:
        raise RuntimeError("Profile requires a fresh stack: " + ", ".join(dirty))
    producer_command = ["node", "load-tests/producer.mjs", "--rate", str(rate),
        "--duration-seconds", str(args.duration_seconds), "--payload-bytes", str(args.payload_bytes),
        "--max-in-flight", str(args.max_in_flight), "--timeout-ms", str(args.timeout_ms),
        "--manifest", args.manifest,
        "--run-id", run_id, "--instance-id", "one", "--output", str(raw)]
    environment = {**os.environ, "RELAYFORGE_ENV_FILE": str(env_file)}
    started = time.monotonic()
    producer = subprocess.Popen(producer_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True, env=environment)
    samples = []
    deadline = started + args.duration_seconds + args.timeout_ms / 1000 + 30
    while producer.poll() is None and time.monotonic() < deadline:
        samples.append(snapshot(compose, settings))
        time.sleep(min(args.sample_interval_seconds, max(0, deadline - time.monotonic())))
    if producer.poll() is None:
        producer.kill()
    stdout, stderr = producer.communicate(timeout=10)
    after = snapshot(compose, settings)
    records = [json.loads(line) for line in raw.read_text().splitlines()] if raw.exists() else []
    producer_summary = json.loads(stdout) if producer.returncode == 0 and stdout.strip() else None
    scheduled = int(rate * args.duration_seconds)
    summary = summarize_records(records, scheduled)
    drain_seconds, drain_samples, after = observe_drain(
        after, lambda: snapshot(compose, settings), args.drain_seconds)
    lag = safe_probe(lambda: json.loads(sql(compose, "postgres", "relayforge_delivery",
        DELIVERY_LAG_SQL % before["time_utc"])))
    if "probe_error" in lag:
        lag = None
    evaluation = evaluate(summary, rate, args.duration_seconds, lag, drain_seconds)
    report = {"profile": args.profile, "target_rate": rate,
        "duration_seconds": args.duration_seconds, "payload_bytes": args.payload_bytes,
        "max_in_flight": args.max_in_flight, "run_id": run_id,
        "producer_command": producer_command, "producer_exit_code": producer.returncode,
        "producer_error_present": bool(stderr), "producer_summary": producer_summary,
        "summary": summary, "before": before, "during": samples, "after": after,
        "drain_samples": drain_samples, "drain_seconds": drain_seconds,
        "delivery_lag": lag, "evaluation": evaluation,
        "raw_results": str(raw)}
    report_path = output_dir / f"{args.profile}-{run_id}.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"report": str(report_path), "status": evaluation["status"],
                      "accepted": summary["accepted"], "scheduled": scheduled}))
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=PROFILES, required=True)
    parser.add_argument("--duration-seconds", type=int, required=True)
    parser.add_argument("--payload-bytes", type=int, default=1024)
    parser.add_argument("--max-in-flight", type=int, default=300)
    parser.add_argument("--sample-interval-seconds", type=int, default=5)
    parser.add_argument("--drain-seconds", type=int, default=120)
    parser.add_argument("--timeout-ms", type=int, default=10000)
    parser.add_argument("--env-file", default=".env.loadtest")
    parser.add_argument("--manifest", default="load-tests/local.credentials.json")
    parser.add_argument("--project", default="relayforge-j3-benchmark")
    parser.add_argument("--output-dir", default="load-tests/results")
    args = parser.parse_args()
    if (args.duration_seconds < 1 or args.duration_seconds > 86400
            or args.sample_interval_seconds < 1 or args.drain_seconds < 1
            or args.max_in_flight < 1 or args.payload_bytes < 100
            or args.payload_bytes > 262144 or args.timeout_ms < 1):
        parser.error("Invalid bounded profile settings")
    if shutil.disk_usage(".").free < 20 * 1024 ** 3:
        parser.error("At least 20 GiB of free host disk is required")
    run_profile(args)


if __name__ == "__main__":
    main()

"""Validate and combine sanitized results from coordinated J3 producer instances."""

import argparse
import json
import math
import re
from collections import Counter
from datetime import datetime
from pathlib import Path

IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,47}$")
MAX_SCHEDULED = 12000000
MAX_START_SKEW_MS = 500


def utc_milliseconds(value):
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError("Invalid UTC start timestamp")
    try:
        return datetime.fromisoformat(value[:-1] + "+00:00").timestamp() * 1000
    except ValueError as error:
        raise ValueError("Invalid UTC start timestamp") from error


def count(value, name):
    if type(value) is not int or value < 0:
        raise ValueError(f"Invalid {name}")
    return value


def aggregate_instances(plan, runs):
    if (not isinstance(plan, dict) or plan.get("version") != 1
            or not isinstance(plan.get("run_id"), str)
            or not IDENTIFIER.fullmatch(plan["run_id"])):
        raise ValueError("Invalid distributed run plan")
    rate = count(plan.get("expected_rate"), "expected rate")
    duration = count(plan.get("duration_seconds"), "duration")
    if rate < 1 or duration < 1 or rate * duration > MAX_SCHEDULED:
        raise ValueError("Invalid distributed run size")
    planned_payload = count(plan.get("payload_bytes"), "payload size")
    if planned_payload < 1 or planned_payload > 262144:
        raise ValueError("Invalid payload size")
    planned_start = utc_milliseconds(plan.get("start_at_utc"))
    planned_instances = plan.get("instances")
    if not isinstance(planned_instances, list) or not 1 <= len(planned_instances) <= 256:
        raise ValueError("Invalid producer instance plan")
    if any(not isinstance(item, dict) for item in planned_instances):
        raise ValueError("Invalid producer instance plan")
    identifiers = [item.get("instance_id") for item in planned_instances]
    if (any(not isinstance(item, str) or not IDENTIFIER.fullmatch(item) for item in identifiers)
            or len(set(identifiers)) != len(identifiers) or set(runs) != set(identifiers)):
        raise ValueError("Invalid or duplicate producer instance")

    scheduled = completed = accepted = rejected = errors = skipped = accepted_in_window = 0
    sent_after_window = 0
    maximum_sent_offset = 0
    requested_rate = 0
    payload_bytes = None
    started = []
    accepted_latencies = []
    rejections = Counter()
    error_codes = Counter()
    details = []
    for instance_id in identifiers:
        run = runs[instance_id]
        summary = run["summary"]
        if summary.get("run_id") != plan["run_id"] or summary.get("instance_id") != instance_id:
            raise ValueError("Mismatched producer run or instance")
        if summary.get("planned_start_at_utc") != plan["start_at_utc"]:
            raise ValueError("Mismatched planned start")
        actual_start = utc_milliseconds(summary.get("started_at_utc"))
        if abs(actual_start - planned_start) > MAX_START_SKEW_MS:
            raise ValueError("Producer start missed planned start")
        if utc_milliseconds(summary.get("finished_at_utc")) < actual_start:
            raise ValueError("Producer finished before its start")
        started.append(actual_start)
        if summary.get("requested_duration_seconds") != duration:
            raise ValueError("Mismatched producer duration")
        shard_rate = count(summary.get("requested_rate"), "producer rate")
        if shard_rate < 1:
            raise ValueError("Invalid producer rate")
        requested_rate += shard_rate
        shard_payload = count(summary.get("payload_bytes"), "payload size")
        if payload_bytes is not None and shard_payload != payload_bytes:
            raise ValueError("Mismatched payload size")
        if shard_payload != planned_payload:
            raise ValueError("Mismatched planned payload size")
        payload_bytes = shard_payload
        shard_scheduled = count(summary.get("scheduled"), "scheduled count")
        shard_completed = count(summary.get("completed"), "completed count")
        shard_skipped = count(summary.get("skipped"), "skipped count")
        if shard_scheduled != shard_rate * duration or shard_completed + shard_skipped != shard_scheduled:
            raise ValueError("Inconsistent producer schedule")
        observed = Counter()
        seen_sequences = bytearray(shard_scheduled)
        seen_count = 0
        for record in run["records"]:
            sequence = count(record.get("sequence"), "sequence")
            if sequence >= shard_scheduled or seen_sequences[sequence]:
                raise ValueError("Duplicate or out-of-range producer sequence")
            seen_sequences[sequence] = 1
            seen_count += 1
            outcome = record.get("outcome")
            if outcome not in ("accepted", "rejected", "error"):
                raise ValueError("Invalid producer outcome")
            sent = record.get("sent_offset_ms")
            finished = record.get("completed_offset_ms")
            latency = record.get("latency_ms")
            if any(type(value) not in (int, float) or not math.isfinite(value)
                   for value in (sent, finished, latency)) or sent < 0 \
                    or finished < sent or latency < 0 or abs(finished - sent - latency) > 2:
                raise ValueError("Invalid producer timing")
            maximum_sent_offset = max(maximum_sent_offset, sent)
            if actual_start - planned_start + sent > duration * 1000:
                sent_after_window += 1
            if outcome == "accepted":
                if record.get("status") != 202:
                    raise ValueError("Invalid accepted status")
                accepted_latencies.append(latency)
                if 0 <= actual_start - planned_start + finished <= duration * 1000:
                    accepted_in_window += 1
            elif outcome == "rejected":
                status = record.get("status")
                if type(status) is not int or status == 202 or status < 100 or status > 599:
                    raise ValueError("Invalid rejected status")
                rejections[str(status)] += 1
            else:
                if record.get("status") is not None:
                    raise ValueError("Invalid transport error status")
                code = record.get("error_code")
                if not isinstance(code, str) or not re.fullmatch(r"[A-Z0-9_]{1,32}", code):
                    raise ValueError("Invalid transport error code")
                error_codes[code] += 1
            observed[outcome] += 1
        if seen_count != shard_completed or any(
                observed[outcome] != summary.get(field) for outcome, field in
                (("accepted", "accepted"), ("rejected", "rejected"), ("error", "errors"))):
            raise ValueError("Producer summary disagrees with raw records")
        scheduled += shard_scheduled
        completed += shard_completed
        accepted += observed["accepted"]
        rejected += observed["rejected"]
        errors += observed["error"]
        skipped += shard_skipped
        details.append({"instance_id": instance_id, "requested_rate": shard_rate,
                        "started_at_utc": summary["started_at_utc"],
                        "finished_at_utc": summary["finished_at_utc"],
                        "scheduled": shard_scheduled, "completed": shard_completed,
                        "accepted": observed["accepted"], "rejected": observed["rejected"],
                        "errors": observed["error"], "skipped": shard_skipped})
    if requested_rate != rate or scheduled != rate * duration:
        raise ValueError("Producer rates do not match planned rate")
    skew = round(max(started) - min(started))
    if skew > MAX_START_SKEW_MS:
        raise ValueError("Producer start skew exceeds limit")
    accepted_latencies.sort()
    def percentile(fraction):
        if not accepted_latencies:
            return None
        return accepted_latencies[max(0, math.ceil(len(accepted_latencies) * fraction) - 1)]
    return {"version": 1, "run_id": plan["run_id"],
            "planned_start_at_utc": plan["start_at_utc"],
            "start_skew_ms": skew, "payload_bytes": payload_bytes,
            "requested_rate": rate, "duration_seconds": duration,
            "instances": details,
            "summary": {"scheduled": scheduled, "completed": completed,
                        "accepted": accepted, "rejected": rejected,
                        "transport_errors": errors, "skipped": skipped,
                        "accepted_within_window": accepted_in_window,
                        "sent_after_window": sent_after_window,
                        "max_sent_offset_ms": maximum_sent_offset,
                        "accepted_latency_ms": {
                            "p50": percentile(0.5), "p95": percentile(0.95),
                            "p99": percentile(0.99)},
                        "rejections": dict(rejections), "errors": dict(error_codes)}}


def read_records(path):
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            if len(line) > 4096:
                raise ValueError("Oversized producer record")
            yield json.loads(line)


def aggregate_from_plan(plan_path):
    plan_path = Path(plan_path).resolve()
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    if not isinstance(plan, dict) or not isinstance(plan.get("instances"), list):
        raise ValueError("Invalid distributed run plan")
    runs = {}
    for item in plan["instances"]:
        if not isinstance(item, dict):
            raise ValueError("Invalid producer instance plan")
        instance_id = item["instance_id"]
        if instance_id in runs:
            raise ValueError("Duplicate producer instance")
        summary_path = (plan_path.parent / item["summary"]).resolve()
        records_path = (plan_path.parent / item["records"]).resolve()
        runs[instance_id] = {"summary": json.loads(summary_path.read_text(encoding="utf-8")),
                             "records": read_records(records_path)}
    return aggregate_instances(plan, runs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    report = aggregate_from_plan(args.plan)
    with Path(args.output).open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    print(json.dumps({"output": args.output, "scheduled": report["summary"]["scheduled"],
                      "accepted": report["summary"]["accepted"]}))


if __name__ == "__main__":
    main()

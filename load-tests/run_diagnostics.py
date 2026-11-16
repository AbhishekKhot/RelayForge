"""Run bounded, independent single-host J3 diagnostics in detached Compose."""

import argparse
import json
import os
import subprocess
import time
from pathlib import Path
from uuid import uuid4

from benchmark import compose_args, sql


def diagnostic_plan(prefix, controlled=False):
    windows = (("controlled", 20),) if controlled else (
        ("normal", 5), ("peak", 3), ("headroom", 2))
    return [{"profile": profile, "duration_seconds": duration,
             "repeat": repeat, "project": f"{prefix}-{profile}-{repeat}"}
            for profile, duration in windows
            for repeat in (1, 2)]


def diagnostics_failed(results):
    return any("error_type" in result for result in results)


def checked(args, timeout, environment=None):
    result = subprocess.run(args, capture_output=True, text=True,
                            timeout=timeout, env=environment)
    if result.returncode:
        raise RuntimeError(f"{args[0]} failed with exit {result.returncode}")
    return result.stdout.strip()


def wait_for_projection(compose, expected, timeout_seconds=120):
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        count = int(sql(compose, "postgres", "relayforge_delivery",
                        "SELECT count(*) FROM subscription_projection WHERE status = 'ACTIVE'"))
        if count == expected:
            return
        if count > expected:
            raise RuntimeError("Unexpected extra active subscriptions")
        time.sleep(2)
    raise RuntimeError("Subscription projection did not reach expected fan-out")


def run_one(item, env_file, output_dir):
    compose = compose_args(env_file, item["project"])
    manifest = output_dir / f"{item['profile']}-{item['repeat']}.credentials.json"
    if checked(compose + ["ps", "-a", "-q"], timeout=15):
        raise RuntimeError("Diagnostic project already has containers")
    environment = {**os.environ, "RELAYFORGE_ENV_FILE": str(env_file)}
    print(f"Starting {item['profile']} repeat {item['repeat']} in detached Compose", flush=True)
    try:
        checked(compose + ["up", "-d", "--build", "--wait", "--wait-timeout", "600"],
                timeout=1200)
        checked(["node", "load-tests/setup.mjs", "--tenants", "15", "--fanout", "3",
                 "--output", str(manifest)], timeout=300, environment=environment)
        wait_for_projection(compose, 45)
        output = checked(["python3", "load-tests/benchmark.py", "--profile", item["profile"],
            "--duration-seconds", str(item["duration_seconds"]), "--drain-seconds", "120",
            "--max-in-flight", "1000", "--payload-bytes", "1024",
            "--env-file", str(env_file), "--manifest", str(manifest),
            "--project", item["project"], "--output-dir", str(output_dir)], timeout=600)
        result = json.loads(output)
        print(json.dumps({"profile": item["profile"], "repeat": item["repeat"], **result}),
              flush=True)
        return result
    finally:
        checked(compose + ["down", "-v", "--remove-orphans"], timeout=300)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", default=".env.benchmark")
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--only", choices=("normal", "peak", "headroom", "controlled"))
    args = parser.parse_args()
    env_file = Path(args.env_file).resolve()
    if not env_file.exists():
        parser.error("Ignored benchmark env file is missing")
    run_id = uuid4().hex[:8]
    output_dir = Path(args.output_dir or f"load-tests/results/diagnostic-{run_id}")
    output_dir.mkdir(parents=True, exist_ok=False)
    plan = diagnostic_plan(f"relayforge-j3-{run_id}", controlled=args.only == "controlled")
    if args.only:
        plan = [item for item in plan if item["profile"] == args.only]
    results = []
    for item in plan:
        try:
            results.append(run_one(item, env_file, output_dir))
        except (OSError, RuntimeError, subprocess.TimeoutExpired, ValueError) as error:
            results.append({"profile": item["profile"], "repeat": item["repeat"],
                            "error_type": type(error).__name__})
            print(json.dumps(results[-1]), flush=True)
    (output_dir / "index.json").write_text(json.dumps({"plan": plan, "runs": results}, indent=2) + "\n")
    print(json.dumps({"index": str(output_dir / 'index.json'),
                      "completed": len(results)}), flush=True)
    if diagnostics_failed(results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()

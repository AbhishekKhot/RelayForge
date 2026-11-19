# J3 load generators

These dependency-free Node.js 22+ tools prepare a workload, submit events, and simulate webhook receivers. They produce raw observations, not capacity claims. HLD §13's 5,000/10,000/15,000 accepted events/s and 3× fan-out are planning inputs. The benchmark task will define run durations, thresholds, environment, and repeated profiles.

## Configure and start

Use a disposable environment. Copy the ignored `.env` to an ignored `.env.loadtest`. In `.env.loadtest`, set `WEBHOOK_SIMULATOR_URL=http://load-receiver:8080` so the existing local SSRF exception and delivery worker both use the load receiver. Set `LOAD_RECEIVER_LATENCY_MS` (0–60000), `LOAD_RECEIVER_FAILURE_PERCENT` (0–100), and optionally `LOAD_RECEIVER_HOST_PORT` (default 18085). The receiver returns 503 for the configured proportion, using a deterministic request-count schedule; 503 is retryable, so this controls attempt mix, not the fraction of logical deliveries that ultimately fail. Changes to receiver settings require recreating its container.

Start Compose in detached mode:

```sh
docker compose --env-file .env.loadtest -f compose.yaml -f deploy/ingestion-ha/compose.yaml -f load-tests/compose.yaml up -d --build --wait --wait-timeout 600
```

Create organizations and endpoint subscriptions. `--fanout` creates that many endpoints per organization for one event type. The tool reads the trusted provisioning credential and receiver URL from the selected ignored env file; it writes API keys only to an ignored file with owner-only permissions. Use a new output path for each setup. The current control-plane policy permits at most 60,000 events/minute and 1,000 active endpoints per organization; use enough organizations to spread the intended request rate and do not treat their nominal limits as measured throughput.

```sh
RELAYFORGE_ENV_FILE=.env.loadtest node load-tests/setup.mjs --tenants 2 --fanout 3 --output load-tests/local.credentials.json
```

`--endpoints-per-tenant` defaults to `--fanout`. Set it higher to populate active endpoints that subscribe to a separate, unused event type. This preserves the requested delivery fan-out while exercising a larger endpoint/configuration population. For the HLD planning population of 100 organizations, 10,000 active endpoints, and three matching subscriptions per event, use `--tenants 100 --fanout 3 --endpoints-per-tenant 100`. This creates 100 endpoints per organization, of which three match the producer's event type. Allow setup and configuration projection to finish before a measured profile; the command only provisions data and does not prove receiver or service capacity.

Run one producer process with a requested open-loop arrival rate. Each process needs a distinct `--instance-id` if you provide the same `--run-id` to several processes. The producer reads the manifest and ingestion port from local files, sends exact-size JSON bodies with unique idempotency keys, and writes one result record per completed request. `--max-in-flight` caps its own concurrency; arrivals above the cap are counted as `skipped`, not queued. Only HTTP 202 counts as accepted. HTTP rejections, transport errors, skipped arrivals, and actual accepted events/s must be reported alongside the requested rate.

```sh
RELAYFORGE_ENV_FILE=.env.loadtest node load-tests/producer.mjs --rate 100 --duration-seconds 30 --payload-bytes 1024 --max-in-flight 500 --manifest load-tests/local.credentials.json
```

The producer stops scheduling after the requested duration, waits for its bounded in-flight requests, prints a summary, and writes raw NDJSON under ignored `load-tests/results/`. `--timeout-ms` defaults to 10000. The size flag is total UTF-8 JSON request bytes, capped by ingestion's 256 KiB request limit. Keep generator machines, replica counts, resource limits, versions, commands, receiver settings, and raw results for the later benchmark report. A single Node process is not assumed to generate a target profile; distribute producers and verify their achieved rates.

## Coordinated producer evidence

For a distributed run, put `INGESTION_API_URL` in each generator's ignored env file. It overrides the local host-port URL and must point to the intended ingestion entry point. Keep the API-key manifest on each generator in an ignored, owner-readable file. Synchronize and measure the hosts' UTC clocks before the run. Choose one UTC start no more than five minutes ahead and give every producer the same `--run-id` and `--start-at-utc`, with a unique `--instance-id` and output path. Rates across instances must sum to the planned rate; use separate generators with measured headroom. The producer reports the actual UTC start and monotonic sent/completed offsets for each request. [Node.js timers](https://nodejs.org/api/timers.html) do not promise exact firing time, so the aggregator rejects a run with a start more than 500 ms from the plan or more than 500 ms of observed inter-instance skew. Clock synchronization remains an external prerequisite.

For example, two generators can split the 5,000/s normal profile into 2,500/s each for 1,800 seconds. Execute the same command on each machine with its own instance ID (`a` or `b`), after setting `START_AT` to one shared future UTC timestamp:

```sh
RELAYFORGE_ENV_FILE=.env.loadtest node load-tests/producer.mjs \
  --rate 2500 --duration-seconds 1800 --payload-bytes 1024 \
  --max-in-flight 5000 --timeout-ms 10000 \
  --manifest load-tests/local.credentials.json \
  --run-id j3-normal-001 --instance-id a --start-at-utc "$START_AT" \
  --output load-tests/results/a.ndjson > load-tests/results/a.json
```

The example concurrency cap is an input, not measured generator capacity. Copy only the sanitized `a.json`/`a.ndjson` and `b.json`/`b.ndjson` to one evidence directory. Create a safe plan in that directory with the **same actual** future start value used by both commands:

```json
{
  "version": 1,
  "run_id": "j3-normal-001",
  "start_at_utc": "REPLACE_WITH_SHARED_FUTURE_UTC_START",
  "expected_rate": 5000,
  "duration_seconds": 1800,
  "payload_bytes": 1024,
  "instances": [
    {"instance_id": "a", "summary": "a.json", "records": "a.ndjson"},
    {"instance_id": "b", "summary": "b.json", "records": "b.ndjson"}
  ]
}
```

```sh
python3 load-tests/aggregate_producers.py \
  --plan load-tests/results/plan.json \
  --output load-tests/results/producer-aggregate.json
```

The aggregator rejects missing or duplicate sequences, inconsistent summaries, mixed run IDs, payload sizes or durations, incorrect total rate, malformed timings, and late/skewed starts. It combines accepted-event p50/p95/p99 from raw records and reports rejected and transport-error counts, skipped arrivals, sends after the common window, and how many `202` responses finished before that window closed. It keeps only accepted latencies in memory and caps one run at 12 million scheduled requests. These are **producer-only observations**. A J3 release result also needs service-side delivery lag, complete backlog drain, topology, hardware, partitions, database configuration, receiver mix, retries, and independent repeats from [the full benchmark plan](../docs/benchmarks/j3-plan.md). The peak and headroom durations in that plan are 300 and 120 seconds; the normal duration is 1,800 seconds. Do not infer a passing profile from the producer aggregate alone.

The receiver exposes `GET /health` and `GET /__stats` (request, success, failure, and byte counts) on its host port. It accepts webhook `POST /load/*`. Treat the stats as observations, not authoritative delivery state. Stop the stack with the same three Compose files, `--env-file .env.loadtest`, and `down` when finished; named volumes remain unless explicitly removed.

## Tests and implementation references

Run `node --test load-tests/load.test.mjs`. The tools use [Node.js HTTP agents](https://nodejs.org/api/http.html) for bounded keep-alive connections, [the built-in test runner](https://nodejs.org/api/test.html), and a monotonic [performance clock](https://nodejs.org/api/perf_hooks.html) for elapsed time. RelayForge's `202` ingestion contract, quota policy, and endpoint registration contract determine the request format and setup behavior.

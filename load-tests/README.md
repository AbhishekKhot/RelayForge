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

Run one producer process with a requested open-loop arrival rate. Each process needs a distinct `--instance-id` if you provide the same `--run-id` to several processes. The producer reads the manifest and ingestion port from local files, sends exact-size JSON bodies with unique idempotency keys, and writes one result record per completed request. `--max-in-flight` caps its own concurrency; arrivals above the cap are counted as `skipped`, not queued. Only HTTP 202 counts as accepted. HTTP rejections, transport errors, skipped arrivals, and actual accepted events/s must be reported alongside the requested rate.

```sh
RELAYFORGE_ENV_FILE=.env.loadtest node load-tests/producer.mjs --rate 100 --duration-seconds 30 --payload-bytes 1024 --max-in-flight 500 --manifest load-tests/local.credentials.json
```

The producer stops scheduling after the requested duration, waits for its bounded in-flight requests, prints a summary, and writes raw NDJSON under ignored `load-tests/results/`. `--timeout-ms` defaults to 10000. The size flag is total UTF-8 JSON request bytes, capped by ingestion's 256 KiB request limit. Keep generator machines, replica counts, resource limits, versions, commands, receiver settings, and raw results for the later benchmark report. A single Node process is not assumed to generate a target profile; distribute producers and verify their achieved rates.

The receiver exposes `GET /health` and `GET /__stats` (request, success, failure, and byte counts) on its host port. It accepts webhook `POST /load/*`. Treat the stats as observations, not authoritative delivery state. Stop the stack with the same three Compose files, `--env-file .env.loadtest`, and `down` when finished; named volumes remain unless explicitly removed.

## Tests and implementation references

Run `node --test load-tests/load.test.mjs`. The tools use [Node.js HTTP agents](https://nodejs.org/api/http.html) for bounded keep-alive connections, [the built-in test runner](https://nodejs.org/api/test.html), and a monotonic [performance clock](https://nodejs.org/api/perf_hooks.html) for elapsed time. RelayForge's `202` ingestion contract, quota policy, and endpoint registration contract determine the request format and setup behavior.

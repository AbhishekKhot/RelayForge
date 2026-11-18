# J3 progress collector cost, 2026-11-19

**Finding:** Two fresh controlled runs with progress-only load and drain samples reduced median collector snapshot wall time from about 11.3 seconds in the prior same-configuration runs to 1.35–1.38 seconds. Both runs accepted every scheduled event and drained, but delivery lag still missed the J3 thresholds. This single-host comparison does not establish production capacity or isolate the collector's effect from day-to-day host variance.

## Method

The source base was the [PR #57](https://github.com/AbhishekKhot/RelayForge/pull/57) merge at `9f603d1`. The prior batch-32 runs in [the fact-batch report](j3-clickhouse-fact-batch.md) used a detailed collector snapshot during load and drain. This change keeps detailed snapshots before and after the profile, including service timers, pool gauges, Redis, ClickHouse, Docker stats, and receiver data. During load and drain, each progress snapshot reads only Kafka group lag and the PostgreSQL counts needed for the existing `pipeline_drained` decision. The final progress snapshot and detailed after snapshot are separately recorded so the drain clock does not include optional diagnostics.

Each new run used a fresh detached Compose project with its own volumes. The profile scheduled 50 events/s for 20 seconds, with 15 organizations, three matching endpoints each, 1 KiB request bodies, a 200 ms fixed receiver delay, 10% deterministic retryable receiver responses, and a 120-second post-producer drain cap. Worker settings were 8 handlers and an 8-connection pool. Analytics used a 32-record Kafka poll and opt-in fact batch size 32 in both the prior and new runs. The host was an Apple M4 MacBook Air with 24 GB RAM; Docker Desktop 29.9.2 allocated 10 CPUs and 8,319,504,384 bytes of memory. No other RelayForge benchmark or Maven build overlapped these runs. Other host load and normal run-to-run variation were not controlled.

The [data index](j3-progress-collector-2026-11-19/index.json) links both sanitized JSON reports and per-request NDJSON. Credential manifests are excluded. Reproduce with the ignored benchmark env using the settings above:

```sh
python3 load-tests/run_diagnostics.py --env-file .env.benchmark --only controlled
```

## Results

| Collector / repeat | Progress samples | Median / maximum sample wall time | Sum of sample wall time | Accepted / scheduled | Accepted p95 / p99 | Delivery lag p95 / p99 | Drain |
|---|---:|---:|---:|---:|---:|---:|---:|
| Prior detailed / 1 | 8 | 11.26 / 22.46 s | 100.1 s | 950 / 1,000 | 3,449 / 3,955 ms | 102.1 / 117.5 s | 112.7 s |
| Prior detailed / 2 | 9 | 11.26 / 17.26 s | 102.3 s | 1,000 / 1,000 | 1,694 / 2,443 ms | 105.6 / 125.1 s | 125.1 s |
| Progress / 1 | 15 | 1.35 / 2.92 s | 22.7 s | 1,000 / 1,000 | 170 / 947 ms | 78.0 / 82.8 s | 76.1 s |
| Progress / 2 | 15 | 1.38 / 3.03 s | 24.8 s | 1,000 / 1,000 | 396 / 987 ms | 80.6 / 85.3 s | 76.7 s |

Both new runs had no generator skips, HTTP rejections, or transport errors. Their final detailed snapshots showed zero delivery outbox backlog and zero analytics Kafka lag. The detailed before/after snapshots still took 6–8 seconds, consistent with the probes removed from the progress path. More progress samples were collected because each sample finished sooner. Probe wall times include Docker Compose commands and SQL work, and the collector shares this host with the application. The large change in measured sampling cost supports using progress-only samples for subsequent diagnostics; it does not prove that the collector alone caused the lower request or delivery latency.

The new runs met the controlled admission, accepted latency, and 120-second drain thresholds. They still missed delivery lag p95 ≤ 5 seconds and p99 ≤ 30 seconds. These were 50 events/s local diagnostics, not the full-duration HLD §13 5,000/10,000/15,000 accepted-events/s profiles.

The progress snapshot behavior test failed before implementation and passed afterward. All 14 benchmark Python tests, Compose configuration validation, both bounded detached runs, and Java 21 `TZ=UTC mvn -B -q verify` passed locally.

## Next task

Use this lower-cost collector to separate delivery publication, worker, and analytics throughput on documented distributed resources. Run the full-duration HLD §13 profiles only when that topology is ready. Retain the one-fact analytics default and treat any unmet threshold as a finding; production shard counts and concurrency caps remain unverified.

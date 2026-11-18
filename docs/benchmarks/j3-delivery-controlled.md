# J3 controlled delivery diagnostic, 2026-11-16

**Finding:** On this single-host Compose stack, a 50 accepted-events/s workload caused substantial delivery lag even when the generator sent every scheduled event. All 1,000 events were accepted and routed in each independent repeat, creating 3,000 deliveries per run. Delivery completion lag p95 was 82.4 and 91.4 seconds. This diagnoses a local delivery-side throughput limit; it does not establish a production capacity ceiling or satisfy the HLD §13 normal profile.

## Reproduction

The new `controlled` mode runs two fresh Compose projects. Each sends 50 events/s for 20 s, then observes the pipeline for at most 120 s. It uses the same 15 organizations, three matching endpoints per organization, 1 KiB event body, 200 ms fixed receiver delay, 10% deterministic retryable receiver responses, 1,000 maximum client in-flight requests, and delivery platform claim cap 128 as [the earlier local diagnostic](j3-local-diagnostics.md). Set those values in an ignored `.env.benchmark` as documented there; credentials and connection URLs remain local. Run:

```sh
python3 -m unittest discover -s load-tests -p 'benchmark_test.py'
python3 load-tests/run_diagnostics.py --env-file .env.benchmark --only controlled
```

The runner starts Compose detached, bounds startup, load, drain, and teardown, checks that 45 subscriptions are active, and removes only each project's own volumes. The source base is the [PR #51](https://github.com/AbhishekKhot/RelayForge/pull/51) merge at `5527524`, plus the collector changes in this PR. The host was an Apple M4 MacBook Air with 24 GB RAM; Docker Desktop 29.9.2 allocated 10 CPUs and 8,319,504,384 bytes RAM. Compose 5.6.1, Node 26.0.0, Python 3.9.6, Java 21.0.7, PostgreSQL 17.11, Kafka 4.1.2, Redis 7.4.8-alpine, and ClickHouse 26.8.3.105 were used. Other workloads may share the host.

The [data index](j3-delivery-controlled-2026-11-16/index.json) maps each checked-in report to its sanitized per-request NDJSON. Reports retain the original ignored `load-tests/results/diagnostic-a179880b/` paths and include exact producer commands, all snapshots, percentiles, errors, WAL and database counters, Kafka lag, Redis counters, ClickHouse counters, and Docker resource samples. Credential manifests are excluded.

## Results

| Repeat | Accepted / scheduled | Skipped / rejected / transport errors | Accepted p95 / p99 | Deliveries terminal at final sample | Attempts | Delivery lag p95 / p99 | Final Kafka analytics lag |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1,000 / 1,000 | 0 / 0 / 0 | 340 / 721 ms | 3,000 / 3,000 | 3,329 | 82.4 / 87.0 s | 0 |
| 2 | 1,000 / 1,000 | 0 / 0 / 0 | 1,159 / 1,979 ms | 3,000 / 3,000 | 3,330 | 91.4 / 95.0 s | 1,613 |

The receiver recorded 3,329 and 3,330 requests after the start snapshot, including 332 and 333 simulated failures. Three logical deliveries were dead-lettered in each run. The actual attempt ratio was about 1.11 per logical delivery, close to the HLD's 1.1 planning assumption for this workload. Accepted counts had no two-run variance, but accepted p95 varied by 819 ms and delivery lag p95 varied by 9.0 s (10.4% of their two-run mean). Both repeats missed the planned delivery lag p95 ≤ 5 s and p99 ≤ 30 s thresholds. Repeat 2 also missed the proposed acceptance latency p95 ≤ 500 ms and p99 ≤ 1,000 ms thresholds.

After arrivals stopped, the samples show terminal deliveries rising from 1,011 to 2,804 over about 55 s in repeat 1 and from 1,013 to 2,888 over about 60 s in repeat 2, approximately 31–33 terminal deliveries/s. At three deliveries per accepted event, the 50 events/s input adds about 150 deliveries/s during the window. The delivery consumer currently has eight handler threads and its PostgreSQL pool has eight connections. A 200 ms receiver delay alone permits at most 40 serial 200 ms calls/s across eight threads; validation, database and HTTP reference work reduce the available rate further. This is an inference from source configuration and samples, not an isolated measurement of each stage.

Delivery work and delivery outbox rows accumulated during both runs. At the first drain sample, delivery work was 1,989 and 1,987 rows and delivery outbox pending was 4,102 and 5,059 rows. Both reached zero by the final sample, but repeat 2 still had 1,613 analytics-fact messages of [Kafka consumer-group lag](https://kafka.apache.org/41/operations/basic-kafka-operations/). The first run's final snapshot showed zero Kafka lag. The snapshots also recorded zero PostgreSQL lock waiters and deadlocks at the final sample; [PostgreSQL's statistics view](https://www.postgresql.org/docs/17/monitoring-stats.html) defines those counters. Ingestion WAL grew 7.2–7.4 MB and delivery WAL about 50.4 MB per run. ClickHouse active fact-part rows and recent `SELECT` timings are in the raw reports; the latter include collector/setup queries and are not an analytics API query benchmark.

The archived runs used the earlier drain loop. In repeat 1, the last snapshot finished after the nominal 120 s observation boundary and showed a fully drained pipeline, but `drain_seconds` was recorded as null because the loop exited before checking it. Repeat 2 still had analytics Kafka lag at its final sample, so full drain was genuinely unobserved. The collector now checks every completed snapshot before applying the time cutoff and records an observed drain time even if the last snapshot crosses the limit; a result above 120 s still fails the threshold. No archived raw result was altered. Short sequential probes can perturb this small stack, and the two-run variance limits precision.

## Next task

After this PR is reviewed and merged, run a focused worker/publisher experiment at the same no-skip workload. Measure worker active handlers, PostgreSQL pool pressure, outbound request time, and outbox publication by message type; change worker concurrency and pool limits only with bounded tests and repeat the controlled profile. The full-duration 5k/10k/15k profiles require independent producers and documented distributed resources. Keep J3 and the first-release capacity gate open.

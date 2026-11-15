# J3 single-host diagnostic, 2026-11-15

**Result: all six short diagnostic repeats missed the proposed J3 thresholds.** This is a reproducible failure finding for the local Compose environment, not a measured production capacity limit. The full HLD §13 normal, peak, and headroom durations and distributed topology remain untested. No production partition count or delivery concurrency cap follows from these runs.

## Environment and reproduction

- Source: `develop` merge of PR #49 at `b35ee3e`, plus the collector and diagnostic runner in this change.
- Host: Apple M4 MacBook Air, 10 CPU cores, 24 GB RAM, macOS 26.6.2. Docker Desktop 29.9.2, Compose 5.6.1, Docker VM 10 CPUs and 8,319,504,384 bytes RAM. Node 26.0.0, Python 3.9.6, Java 21.0.7, Maven 3.10.0. Other Docker workloads may share the host.
- Compose image tags: PostgreSQL 17.11, Kafka 4.1.2, Redis 7.4.8-alpine, ClickHouse 26.8.3.105, WireMock 3.13.2; RelayForge application and load-receiver images were built locally from this source tree. About 270 GiB of host disk remained after the runs. Local image tags are mutable, so use the recorded source commit and rebuild for reproduction.
- Stack: one Kafka broker, one logical ingestion/delivery shard, one ClickHouse node. Six fresh Compose projects, each removed with `down -v` after its run. No run reused a prior backlog.
- Local workload: 15 organizations, 45 active subscribed endpoints, three deliveries per routed event, 1,024-byte event body, fixed 200 ms receiver delay, 10% HTTP 503 receiver failures, delivery platform claim cap 128. The generator used one local process and 1,000 maximum in-flight requests.
- Arrival windows: normal 5,000 scheduled events/s for 5 s; peak 10,000/s for 3 s; headroom 15,000/s for 2 s. Two repeats each; post-window drain ceiling 120 s. Each profile command, exact run ID, full snapshots, and raw per-request outcomes are in [the data index](j3-local-2026-11-15/index.json). The runner is bounded by a 1,200 s Compose startup limit, a 600 s profile limit, and a 300 s teardown limit.

Create an ignored `.env.benchmark` from the local load-test env, set `LOAD_RECEIVER_LATENCY_MS=200`, `LOAD_RECEIVER_FAILURE_PERCENT=10`, `DELIVERY_PLATFORM_CONCURRENCY_LIMIT=128`, and point `WEBHOOK_SIMULATOR_URL` to the load receiver as documented in `load-tests/README.md`. Keep all credentials and connection URLs there. Then run:

```sh
python3 -m unittest discover -s load-tests -p 'benchmark_test.py'
python3 load-tests/run_diagnostics.py --env-file .env.benchmark
```

The runner starts Compose detached, provisions 15 organizations and 45 subscriptions, checks projection convergence, samples metrics, and removes only its own projects and volumes. The ignored local credential manifests are intentionally absent from the archived evidence. Raw reports are copied unchanged from `load-tests/results/diagnostic-b0ee42a4/`; their `raw_results` and command paths refer to that original local output directory. The archive index maps each report to its checked-in NDJSON file.

## Outcomes

| Profile / repeat | Scheduled | Accepted | Skipped | HTTP 503 | Accepted p95 / p99 | Routed at final sample | Delivery backlog at final sample | Completed delivery lag p95 / p99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Normal 1 | 25,000 | 1,926 | 23,072 | 2 | 4,952 / 5,378 ms | 323 | 737 | 164.7 / 171.9 s |
| Normal 2 | 25,000 | 1,395 | 23,598 | 7 | 6,550 / 6,955 ms | 261 | 598 | 135.2 / 139.8 s |
| Peak 1 | 30,000 | 800 | 29,000 | 200 | 8,698 / 8,748 ms | 258 | 588 | 133.6 / 138.3 s |
| Peak 2 | 30,000 | 1,324 | 28,676 | 0 | 4,877 / 5,242 ms | 257 | 587 | 133.1 / 137.4 s |
| Headroom 1 | 30,000 | 904 | 29,000 | 96 | 7,787 / 7,858 ms | 263 | 599 | 136.0 / 141.7 s |
| Headroom 2 | 30,000 | 999 | 29,000 | 1 | 7,724 / 7,834 ms | 262 | 599 | 135.4 / 140.6 s |

All six had zero recorded transport errors, but substantial generator skips. Every run failed the minimum 99% acceptance ratio, no-skip condition, p95 ≤ 500 ms, p99 ≤ 1,000 ms, delivery lag p95 ≤ 5 s, p99 ≤ 30 s, and 120 s full-pipeline drain. The lag percentiles include **only terminal deliveries** visible at the final sample, between 184 and 241 per run; they understate the lag of unfinished work. The collector correctly leaves drain time unset when accepted events have not all routed and outboxes, delivery work, and Kafka lag have not cleared.

After archiving these runs, the drain predicate was tightened to require every routing row and delivery to be terminal. All archived runs already failed the earlier predicate, so this does not change their outcome; future runs include a `routing_incomplete` count.

Accepted counts ranged 1,395–1,926 for normal (range 32.0% of the two-run mean), 800–1,324 for peak (49.3%), and 904–999 for headroom (10.0%). These short repeats vary materially. Because the generator reached its 1,000 in-flight cap and skipped most scheduled arrivals, the runs did **not** sustain the requested arrival rates; accepted count divided by the short scheduling window is not a sustained throughput estimate.

## Downstream and resource findings

- At the final samples, only 257–323 accepted events had routing records; each had exactly three delivery records. Ingestion outbox pending ranged 1,084–3,205 rows and delivery outbox pending 1,466–1,839. Kafka consumer lag at the final samples was zero or one on the monitored topics. This points to work accumulating before or within outbox publication and delivery processing, but the samples do not isolate a single cause.
- The receiver logged 212–267 requests per run, with 21–26 simulated failures. Delivery attempts numbered 204–258 while most deliveries remained unfinished. The target 1.1 attempts per completed delivery cannot be evaluated from these incomplete pipelines.
- Ingestion WAL grew 4.3–9.8 MB and delivery WAL 5.4–7.4 MB per run. PostgreSQL showed zero deadlocks and zero lock waiters at final samples. Database sizes, commits, active connections, and per-container CPU/memory snapshots are in the report JSON files. A final zero-active Hikari sample is a point-in-time observation, not evidence that pool pressure never occurred.
- Redis hit/miss deltas were 1,585–3,652 / 381–463; final memory use was about 1.5 MB. ClickHouse had 564–714 active fact-part rows at final samples, below delivered facts implied by the accepted population because the pipeline remained incomplete. Recent `SELECT` p95 in `system.query_log` was 5–7 ms; these selects include collector/setup activity and are **not** an analytics query benchmark.
- The collector produced one in-window snapshot per short run. Its sequential Docker/SQL probes perturb this small stack and limit time-series diagnosis. The final samples were obtained after the nominal 120 s drain ceiling when the last snapshot was still in progress; the report stores exact timestamps. Further bottleneck isolation needs targeted outbox/worker instrumentation and an independent load generator.

## Decision

Keep J3 and the first-release capacity gate open. Before proposing production partitions or concurrency caps, run the full-duration contract in [the benchmark plan](j3-plan.md) on a documented distributed environment with producers on separate hosts and enough endpoints to test the intended fan-out. First use smaller controlled rates to find stable throughput and investigate outbox and delivery progress, then repeat all target profiles with no generator skips and a fully observed drain. A failed target remains a finding until the bottleneck is fixed and the profile is rerun.

# J3 delivery publication, ClickHouse, and collector timing, 2026-11-18

**Finding:** Two identical fresh controlled runs at the unchanged 8/8 worker and one-record analytics settings missed delivery lag and full-drain targets. Delivery outbox publication completed, while analytics Kafka lag remained. Publisher claim calls took more cumulative time than Kafka publish or database acknowledgement calls. ClickHouse fact lookup and insert HTTP calls were both substantial per-fact work. Each collector snapshot took a median 11–12 seconds, so this single-host diagnostic cannot isolate service capacity or justify a concurrency change.

## Method

The source base was the [PR #55](https://github.com/AbhishekKhot/RelayForge/pull/55) merge at `33aceb8`, plus read-only timing instrumentation in this PR. Micrometer timers now measure each delivery publisher claim, Kafka publish call, and published-row database update. The analytics fact store separately times its ClickHouse duplicate lookup and insert HTTP calls. The collector records elapsed wall time for the whole snapshot and each sequential probe. The timers include failed calls and do not include envelope encoding, fact JSON parsing, or all scheduler overhead. [Micrometer timer counts and total time](https://docs.micrometer.io/micrometer/reference/concepts/timers.html) are cumulative; averages below divide total time by count. [Kafka consumer-group lag](https://kafka.apache.org/41/operations/basic-kafka-operations/) measures records still to consume after publication.

Each run used a fresh detached Compose project and its own volumes. The runner checked 45 active subscriptions, scheduled 50 events/s for 20 seconds, observed at most 120 seconds of post-producer drain, and removed only its own stack and volumes. Both runs used 15 organizations, three matching endpoints per organization, 1 KiB event bodies, a 200 ms fixed receiver delay, 10% deterministic retryable receiver responses, a 128 platform claim cap, and 1,000 maximum client requests in flight. `DELIVERY_CONSUMER_WORKERS=8`, `DELIVERY_DATABASE_POOL_SIZE=8`, and `RELAYFORGE_ANALYTICS_MAX_POLL_RECORDS=1` remained unchanged. No other J3 run or Maven build overlapped them; other Docker workloads could share the host.

Reproduce from the same ignored `.env.benchmark` settings described in [the controlled diagnostic](j3-delivery-controlled.md):

```sh
python3 load-tests/run_diagnostics.py --env-file .env.benchmark --only controlled
```

The host was an Apple M4 MacBook Air with 24 GB RAM. Docker Desktop 29.9.2 allocated 10 CPUs and 8,319,504,384 bytes of memory; Compose was 5.6.1, Java 21.0.7, Maven 3.10.0, Node 26.0.0, and Python 3.9.6. [The data index](j3-stage-timing-2026-11-18/index.json) maps both sanitized JSON reports and their per-request NDJSON. Reports retain original ignored result paths and contain exact producer commands, per-probe and service timers, percentiles, errors, SQL and Kafka counters, and resource samples. Credential manifests are excluded.

## Results

| Repeat | Accepted / scheduled | Accepted p95 / p99 | Terminal deliveries | Delivery lag p95 / p99 | Final delivery outbox pending | Final analytics Kafka lag | Full drain ≤ 120 s |
|---|---:|---:|---:|---:|---:|---:|---|
| 1 | 1,000 / 1,000 | 745 / 1,067 ms | 3,000 / 3,000 | 101.4 / 113.9 s | 0 | 2,476 | No |
| 2 | 1,000 / 1,000 | 1,110 / 1,945 ms | 3,000 / 3,000 | 114.2 / 133.2 s | 0 | 3,754 | No |

The generator skipped no requests and recorded no HTTP rejection or transport error. Both runs missed accepted-event p95 ≤ 500 ms, p99 ≤ 1,000 ms, delivery lag p95 ≤ 5 s, p99 ≤ 30 s, and full drain ≤ 120 s. The acceptance p95 values differed by 365 ms and delivery lag p95 by 12.8 seconds, showing material shared-host variance even with identical settings.

| Stage | Repeat 1 calls / total / mean | Repeat 2 calls / total / mean |
|---|---:|---:|
| Delivery publisher claim | 13,226 / 59.9 s / 4.53 ms | 13,135 / 75.3 s / 5.73 ms |
| Delivery publisher Kafka publish | 12,987 / 22.8 s / 1.76 ms | 12,984 / 25.7 s / 1.98 ms |
| Delivery publisher database acknowledgement | 12,987 / 16.0 s / 1.23 ms | 12,984 / 17.7 s / 1.36 ms |
| ClickHouse fact duplicate lookup | 8,711 / 59.7 s / 6.86 ms | 7,412 / 58.3 s / 7.87 ms |
| ClickHouse fact insert | 8,711 / 49.0 s / 5.62 ms | 7,413 / 48.9 s / 6.59 ms |

The publisher claim count includes empty polls. These are per-process cumulative timer totals, not independent wall-clock durations; other pipeline stages run concurrently. The final snapshot was taken sequentially, so adjacent counters can differ slightly. Both runs cleared delivery outbox rows, but the analytics consumer still had thousands of Kafka records pending. A duplicate check plus a small synchronous insert is performed for each new fact. [ClickHouse recommends batching small inserts](https://clickhouse.com/blog/common-getting-started-issues-with-clickhouse); any future batch design must retain duplicate detection, conflicting-version handling, and safe Kafka offset replay.

| Collector cost | Repeat 1 | Repeat 2 |
|---|---:|---:|
| Snapshots measured | 11 | 10 |
| Median / maximum snapshot wall time | 11.2 / 14.3 s | 11.9 / 18.2 s |
| Sum of snapshot wall times | 123.9 s | 125.3 s |
| Kafka group probe total | 28.0 s | 29.1 s |
| Docker stats probe total | 25.4 s | 23.4 s |
| Four Actuator probe groups total | 61.3 s | 62.1 s |

The collector runs probes in sequence and performs Docker Compose commands and short-lived metric-query containers. Its elapsed time includes waiting for those commands, not just CPU use. Snapshot work overlapped service processing, so the table measures measurement cost but cannot quantify how much it slowed the services. The prior PR #55 runs used a lighter collector, making a direct performance comparison unsuitable. Future capacity tests need a lower-overhead, distributed metrics collection path and a matched control before interpreting small changes.

## Next task

Use the measured claim and per-fact HTTP costs to design a bounded change to publication or analytics storage, preserving leases, duplicate/conflict semantics, and replay safety. Compare at least two independent fresh runs against a matched unchanged control. Keep 8/8 workers and the one-record analytics default until complete acceptance, delivery, and drain criteria improve. Full-duration HLD §13 5,000/10,000/15,000 accepted-events/s profiles remain unrun on distributed resources.

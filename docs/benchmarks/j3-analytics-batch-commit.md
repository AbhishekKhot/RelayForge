# J3 analytics Kafka commit batch diagnostic, 2026-11-18

**Finding:** A bounded 32-record poll reduced analytics Kafka commit calls by about eight to nine times relative to the one-record control, but it did not improve the complete controlled flow. Neither setting met delivery lag or full-drain thresholds in two independent repeats. Keep the one-record default. The 32-record setting is an opt-in diagnostic control, not a production concurrency or capacity recommendation.

## Method and safety

The source base was the [PR #54](https://github.com/AbhishekKhot/RelayForge/pull/54) merge at `28b0d56`, plus this change. The analytics consumer originally polled one record, inserted one ClickHouse fact, then synchronously committed that record's Kafka offset. The optional setting `RELAYFORGE_ANALYTICS_MAX_POLL_RECORDS` accepts 1–32 and defaults to 1. Within each partition, the consumer now commits the highest contiguous processed offset once after the poll's successful prefix. A failed fact stops that partition's batch, commits only earlier processed records, and seeks back to the failed record. A failed commit seeks back to the first record in the batch. Invalid facts are quarantined before their offsets can be committed. At-least-once replay remains possible. [Kafka's manual commit contract](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html) requires committing the next offset after fully processed records; the tests cover success, middle failure, quarantine, and commit failure.

Each of four runs used a fresh detached Compose project and its own volumes. The unchanged delivery worker had eight handlers and eight PostgreSQL connections. Each run scheduled 50 events/s for 20 seconds, with 15 organizations, three matching endpoints each, 1 KiB bodies, a 200 ms fixed receiver delay, 10% deterministic retryable receiver responses, a 128 platform claim cap, and 1,000 maximum client in-flight requests. Observation after arrivals stopped was bounded to 120 seconds. The runner checked 45 active subscriptions and removed its own containers and volumes. No other build or J3 run overlapped these comparisons, although other Docker workloads could share the host.

To reproduce, set `DELIVERY_CONSUMER_WORKERS=8` and `DELIVERY_DATABASE_POOL_SIZE=8` in an ignored `.env.benchmark`, with the other settings described in [the controlled diagnostic](j3-delivery-controlled.md). Run the command below once with `RELAYFORGE_ANALYTICS_MAX_POLL_RECORDS=1`, then once with `32`. Each invocation makes two independent repeats and has bounded startup, load, drain, and teardown:

```sh
python3 load-tests/run_diagnostics.py --env-file .env.benchmark --only controlled
```

The host was an Apple M4 MacBook Air with 24 GB RAM. Docker Desktop 29.9.2 allocated 10 CPUs and 8,319,504,384 bytes of memory; Compose was 5.6.1, Java 21.0.7, Maven 3.10.0, Node 26.0.0, and Python 3.9.6. [The data index](j3-analytics-batch-2026-11-18/index.json) maps the four sanitized JSON reports and per-request NDJSON files. Reports contain exact producer commands and sequential snapshots. Credential manifests are excluded. A clean Java 21 `TZ=UTC mvn -B -q clean verify`, 12 Python benchmark tests, and Compose configuration validation passed. The first non-clean Maven attempt encountered stale duplicated classes in ignored `target/` output; clean verification removed them and passed without a tracked-code fix.

## Matched results

| Poll records | Repeat | Accepted / scheduled | HTTP 503 | Accepted p95 / p99 | Terminal deliveries at final sample | Delivery lag p95 / p99 | Full drain ≤ 120 s |
|---:|---:|---:|---:|---:|---:|---|
| 1 | 1 | 1,000 / 1,000 | 0 | 356 / 998 ms | 3,000 / 3,000 | 95.9 / 109.6 s | No |
| 1 | 2 | 998 / 1,000 | 2 | 921 / 1,492 ms | 2,994 / 2,994 | 107.4 / 125.6 s | No |
| 32 | 1 | 990 / 1,000 | 10 | 1,551 / 2,017 ms | 2,847 / 2,970 | 105.8 / 145.2 s | No |
| 32 | 2 | 1,000 / 1,000 | 0 | 802 / 1,497 ms | 3,000 / 3,000 | 128.6 / 141.0 s | No |

The generator skipped no requests and recorded no transport errors. The 99% admission ratio was met in every run, including exactly 990 of 1,000 in the first 32-record repeat. The 32-record setting missed the accepted-latency p95 and p99 thresholds in both repeats; the one-record setting missed them in its second repeat. Every run missed delivery lag p95 ≤ 5 s, p99 ≤ 30 s, and full drain ≤ 120 s. The two 32-record delivery p95 values were 10 and 21 seconds above the respective one-record repeats, but the shared-host variance and sequential probes do not establish causation.

| Poll records | Repeat | Final delivery outbox pending | Final analytics Kafka lag | Sum of those two backlogs | Analytics insert operations | Kafka commit calls |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 0 | 2,181 | 2,181 | 9,022 | 9,023 |
| 1 | 2 | 0 | 3,441 | 3,441 | 7,685 | 7,687 |
| 32 | 1 | 3,470 | 199 | 3,669 | 7,385 | 872 |
| 32 | 2 | 1,238 | 665 | 1,903 | 9,441 | 1,177 |

The combined backlog is a diagnostic sum of pending analytics and delivery-ready outbox rows plus analytics Kafka lag; it is not a count of distinct logical deliveries. The displayed sum uses **all** delivery outbox pending rows and analytics Kafka lag, so it includes upstream work that has not yet entered Kafka. Sequential SQL, Actuator, and Kafka probes sample different instants, and the insert and commit counters can differ slightly for that reason. Both 32-record repeats had unprocessed delivery outbox rows aged about 49–75 seconds at the final sample, while both one-record repeats had cleared the outbox. ClickHouse insert-error counters were zero in all four. The store still performs a duplicate check and a small insert for each fact; grouping Kafka commits alone does not change that work. [ClickHouse recommends batching small inserts](https://clickhouse.com/blog/common-getting-started-issues-with-clickhouse), but any future change must preserve the existing duplicate and conflicting-version behavior.

## Next task

Use a separate bounded experiment to isolate delivery outbox publication and ClickHouse fact-store work, including the collector's sampling cost. Compare against the unchanged defaults and require independent end-to-end improvement before raising a default. The full-duration HLD §13 5,000/10,000/15,000 accepted-events/s profiles remain unrun on distributed resources, and production partition counts and concurrency caps remain unverified.

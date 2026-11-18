# J3 delivery worker capacity diagnostic, 2026-11-18

**Finding:** Increasing this single-host worker from 8 handlers / 8 PostgreSQL connections to 16 / 12 did not improve the controlled profile. The matched 8/8 repeats accepted 1,000 and 993 of 1,000 scheduled events; the 16/12 repeats accepted 988 and 947. Delivery lag p95 increased from 114/122 s to 127/129 s. All four runs missed delivery lag and full-drain thresholds. Keep the 8/8 defaults; the new capacity settings are bounded experimental controls, not production sizing evidence.

## Method

The source base was the PR #52 merge, `16419c5`, plus the worker metrics, settings, and collector changes in this PR. Each run used a fresh detached Compose project and its own volumes, 15 organizations with three matching endpoints each, 1 KiB event bodies, an open-loop 50 accepted-events/s target for 20 s, a receiver with fixed 200 ms latency and 10% deterministic retryable responses, a 128 platform claim cap, 1,000 client requests maximum in flight, and at most 120 s of drain observation. The runner checked 45 active subscriptions before sending. It bounded startup, load, drain, and teardown, and removed each run's own containers and volumes. No other build ran during the four matched repeats.

Run the comparison by setting `DELIVERY_CONSUMER_WORKERS` and `DELIVERY_DATABASE_POOL_SIZE` to `8` and `8`, then `16` and `12`, in an ignored `.env.benchmark` with the receiver and credential values described in [the prior diagnostic](j3-delivery-controlled.md). For each setting run:

```sh
python3 -m unittest discover -s load-tests -p benchmark_test.py
python3 load-tests/run_diagnostics.py --env-file .env.benchmark --only controlled
```

The host was an Apple M4 MacBook Air with 24 GB RAM. Docker Desktop 29.9.2 had 10 CPUs and 8,319,504,384 bytes of memory; Compose was 5.6.1, Java 21.0.7, Maven 3.10.0, Node 26.0.0, and Python 3.9.6. The [data index](j3-worker-capacity-2026-11-18/index.json) links the four matched reports and their sanitized per-request NDJSON. It also retains two exploratory 16/12 runs, excluded from the comparison because they predated the dedicated outbound timer and one overlapped a local Maven test. Reports retain original ignored result paths and include exact producer commands, timestamps, snapshots, percentiles, WAL and database counters, Kafka lag, Redis and ClickHouse counters, and Docker resource samples. Credential manifests are excluded.

## Matched results

| Workers / pool | Repeat | Accepted / scheduled | Rejected / skipped | Accepted p95 / p99 | Terminal deliveries at final sample | Delivery lag p95 / p99 | Full drain ≤ 120 s |
|---|---:|---:|---:|---:|---:|---:|---|
| 8 / 8 | 1 | 1,000 / 1,000 | 0 / 0 | 411 / 806 ms | 2,988 / 3,000 | 114 / 136 s | Not observed |
| 8 / 8 | 2 | 993 / 1,000 | 7 / 0 | 3,440 / 4,504 ms | 2,899 / 2,979 | 122 / 140 s | Not observed |
| 16 / 12 | 1 | 988 / 1,000 | 12 / 0 | 3,276 / 4,119 ms | 2,964 / 2,964 | 127 / 143 s | Not observed |
| 16 / 12 | 2 | 947 / 1,000 | 53 / 0 | 3,499 / 4,419 ms | 2,784 / 2,841 | 129 / 142 s | Not observed |

The generator sent every scheduled request. All rejections were HTTP 503; there were no transport errors. The acceptance ratio goal was missed in both 16/12 runs, while the 8/8 runs met it. Acceptance latency varied sharply across repeats, so four single-host observations cannot isolate the reason for the 503s. None approached the proposed delivery lag p95 ≤ 5 s, p99 ≤ 30 s, or full drain ≤ 120 s thresholds. These are workload findings, not evidence for the HLD §13 5,000/10,000/15,000 events/s planning targets.

## Stage observations

| Workers / pool | Repeat | Max sampled active handlers | Max sampled Hikari active / pending | Mean outbound call | Final delivery-ready outbox pending | Final analytics-fact outbox pending | Final analytics Kafka lag |
|---|---:|---:|---:|---:|---:|---:|---:|
| 8 / 8 | 1 | 8 | 5 / 0 | 205 ms | 12 | 248 | 3,890 |
| 8 / 8 | 2 | 8 | 1 / 0 | 206 ms | 80 | 1,536 | 3,259 |
| 16 / 12 | 1 | 8 | 1 / 0 | 205 ms | 0 | 7 | 3,920 |
| 16 / 12 | 2 | 16 | 3 / 0 | 207 ms | 54 | 1,061 | 3,531 |

The outbound timer measures the `SafeWebhookClient.send` call alone, including calls that fail. The existing attempt timer also includes result recording and averaged 212–217 ms; the full handler timer averaged 266–302 ms. Thus roughly 200 ms of each active handler is receiver/HTTP time in this controlled workload. Hikari pending was zero at every sampled point, and its sampled active connections never reached the configured maximum. These sequential probes can miss short spikes, but they offer no measured reason to enlarge the pool. [HikariCP defines `maximumPoolSize` as the connection cap and documents that borrowers wait when it is exhausted](https://github.com/brettwooldridge/HikariCP#maximumPoolSize); [Spring Boot exposes Hikari pool metrics through Actuator](https://docs.spring.io/spring-boot/reference/actuator/metrics.html).

At the final samples, delivery-ready outbox rows remained pending in three runs, analytics-fact outbox rows remained pending in all four, and the analytics Kafka group had 3,259–3,920 messages of lag. This is evidence of downstream drain pressure independent of whether the webhook worker has finished its own HTTP calls. It does not identify a single publisher limit: routing, database writes, publication, analytics consumption, and collector overhead all share this host. Kafka's consumer client is [not thread safe](https://kafka.apache.org/38/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html); the worker retains one poll thread and changes only its bounded handler executor.

The instrumentation adds several actuator reads per snapshot, and each read samples a different instant. The prior PR #52 8/8 run had a lighter collector, so its 82/91 s delivery lag p95 is not a matched control for these results. The matched 8/8 lag also varied by 8 s, and acceptance p95 varied by 3.0 s. No production concurrency cap or partition count should be selected from these runs.

## Next task

After review and merge, isolate delivery outbox publication and analytics consumption with per-stage throughput and age measurements on a controlled stack. Preserve the 8/8 defaults until an independently repeated setting improves the complete acceptance, delivery, and drain criteria. Full-duration HLD §13 profiles still require documented distributed resources.

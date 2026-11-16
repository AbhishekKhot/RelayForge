# J3 ingestion outbox polling investigation, 2026-11-16

**Finding:** The local ingestion publisher processed one row per poll with a 250 ms fixed delay. [Spring's scheduling contract](https://docs.spring.io/spring-framework/reference/integration/scheduling.html) measures that delay after the previous invocation completes, so this configuration imposes a ceiling below four rows per second even before database and Kafka time. The archived normal-rate diagnostics showed only 261–323 of 1,395–1,926 accepted events routed at the final sample, with 2,268–3,205 ingestion outbox rows pending. This is a code-level bottleneck; the earlier data alone did not isolate every downstream cause.

The publisher processes at most 100 rows per poll and starts no new row after five seconds have elapsed; one in-progress row can extend the poll beyond that budget. It claims **one row at a time** through the existing PostgreSQL `FOR UPDATE SKIP LOCKED` query and waits for its Kafka acknowledgement before marking that row published. It stops on an empty queue. This avoids holding leases for a large claimed batch while earlier rows wait for Kafka. The count is configurable with `RELAYFORGE_OUTBOX_MAX_ROWS_PER_POLL` (1–100; default 100). The 250 ms idle poll delay and 30 s per-row lease remain. [PostgreSQL documents `SKIP LOCKED` for queue consumers](https://www.postgresql.org/docs/17/sql-select.html); [Kafka documents that `Future.get()` waits for the send result](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html).

## Reproduction and environment

The comparison uses the same bounded local normal diagnostic as [the previous J3 report](j3-local-diagnostics.md): 5,000 scheduled arrivals/s for 5 s, 15 organizations, 45 subscribed endpoints, 1 KiB event bodies, 200 ms receiver latency, 10% receiver HTTP 503 responses, 1,000 client in-flight maximum, and a 120 s drain ceiling. Each repeat booted an independent detached Compose stack and removed only its own volumes. The source base is `develop` merge `edaa062` of PR #50 plus this publisher change. Docker Desktop 29.9.2 provided 10 CPUs and 8,319,504,384 bytes RAM on an Apple M4 MacBook Air with 24 GB host RAM. Java 21.0.7, Node 26.0.0, Python 3.9.6, and Compose 5.6.1 were used. PostgreSQL 17.11, Kafka 4.1.2, Redis 7.4.8-alpine, and ClickHouse 26.8.3.105 image tags matched the previous local report. Other Docker workloads may share the host.

Reproduce with the ignored `.env.benchmark` configuration described in the [benchmark plan](j3-plan.md):

```sh
python3 load-tests/run_diagnostics.py --env-file .env.benchmark --only normal
```

The exact producer commands, sampled counters, percentiles, failures, and per-request outcomes are archived in [the two-run data index](j3-outbox-poll-2026-11-16/index.json). Reports retain their original ignored `load-tests/results/diagnostic-37a6a17f/` paths; the index maps them to checked-in raw files. No credential manifests or connection URLs are archived.

For the Java verification on this Asia/Kolkata host, use `TZ=UTC` with Java 21. Two existing schema-test fixtures use PostgreSQL `current_date` beside a UTC-day constraint and fail between local midnight and UTC midnight; the same tests pass under UTC. This is a test-fixture follow-up, not a benchmark result.

## Measured comparison

| Local normal repeat | Accepted / scheduled | Routed / accepted at final sample | Ingestion outbox pending | Delivery backlog | Delivery outbox pending | Full drain |
|---|---:|---:|---:|---:|---:|---|
| Previous 1 | 1,926 / 25,000 | 323 / 1,926 (16.8%) | 3,205 | 737 | 1,839 | Unmet |
| Previous 2 | 1,395 / 25,000 | 261 / 1,395 (18.7%) | 2,268 | 598 | 1,488 | Unmet |
| Updated 1 | 2,200 / 25,000 | 2,200 / 2,200 (100%) | 0 | 2,636 | 9,578 | Unmet |
| Updated 2 | 1,303 / 25,000 | 1,303 / 1,303 (100%) | 0 | 192 | 3,946 | Unmet |

The two updated repeats accepted 2,200 and 1,303 requests, with 22,800 and 23,696 generator skips. Their accepted-event p95 latencies were 4,234 and 7,109 ms. Both had zero HTTP rejections and zero transport errors. Delivery completion lag p95 was 130 and 125 s, measured only over deliveries with a completion timestamp at query time. The first updated run had Kafka lag of 1,569 analytics-fact messages and 1,014 delivery-ready messages at its final sample; the second had 2,827 analytics-fact messages and zero delivery-ready messages. These are point-in-time, short-run observations. The updated accepted counts varied by 51.2% of their two-run mean, so they do not show stable admission throughput.

The ingestion-stage result is consistent across both repeats: all accepted events routed and no ingestion outbox rows remained at the final sample. More routing exposes delivery-side work that was previously hidden behind the ingestion bottleneck. Neither repeat met the planned acceptance, latency, delivery lag, or full-drain thresholds. The local single-host setup cannot establish the HLD 5,000 events/s normal goal or production shard and concurrency settings.

## Next investigation

After review and merge, isolate delivery publisher and worker throughput with independent producers and lower arrival rates that do not skip scheduled requests. Record outbox publish counters, claim durations, Kafka acknowledgements, delivery attempts and receiver response times at finer intervals. Then repeat the full-duration profiles on documented distributed resources. Keep J3 and the first-release capacity gate open.

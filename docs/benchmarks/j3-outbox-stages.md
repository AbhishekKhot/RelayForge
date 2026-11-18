# J3 delivery outbox and analytics stage diagnostic, 2026-11-18

**Finding:** The delivery outbox publisher and analytics consumer both accumulated work during two controlled single-host repeats. In repeat 1, the delivery outbox eventually cleared, but the analytics Kafka group still had 2,678 messages of lag. In repeat 2, both outbox types remained backed up at the final sample. This evidence does not support a higher delivery worker or database pool setting, a production capacity claim, or completion of the HLD §13 profiles.

## Method

The source base was the PR #53 merge at `531f141`, plus this collector change. Both runs used the unchanged 8 delivery handlers and 8 worker PostgreSQL connections. Each independent fresh detached Compose project sent 50 scheduled events/s for 20 seconds, with 15 organizations, three matching endpoints each, 1 KiB requests, a receiver with 200 ms fixed delay and 10% deterministic retryable responses, a 128 platform claim cap, and 1,000 client requests maximum in flight. Observation after the producer finished was bounded to 120 seconds. The runner checked 45 active subscriptions, then removed only its own containers and volumes. Other workloads shared the host.

The collector now records delivery outbox counts and oldest pending age separately for `delivery.ready.v1` and `analytics.delivery-facts.v1`. It also records the delivery publisher's publish counter and pending gauges, the analytics consumer's ClickHouse insert count and duration, ingest age, and its Kafka lag gauge. Kafka group offsets are summed by topic when the group query returns numeric positions. Each snapshot probes components sequentially, so differences between counters, SQL rows, and Kafka offsets at one nominal sample are expected. The new fields are read-only measurements; no service behavior changed. [Spring's Actuator metrics endpoint](https://docs.spring.io/spring-boot/api/rest/actuator/metrics.html), [Kafka consumer-group lag](https://kafka.apache.org/41/operations/basic-kafka-operations/), and [PostgreSQL statistics](https://www.postgresql.org/docs/17/monitoring-stats.html) define the underlying measurements.

With the ignored `.env.benchmark` set to `DELIVERY_CONSUMER_WORKERS=8` and `DELIVERY_DATABASE_POOL_SIZE=8` and the other values described in [the controlled diagnostic](j3-delivery-controlled.md), reproduce with:

```sh
python3 -m unittest discover -s load-tests -p benchmark_test.py
python3 load-tests/run_diagnostics.py --env-file .env.benchmark --only controlled
```

The host was an Apple M4 MacBook Air with 24 GB RAM. Docker Desktop 29.9.2 had 10 CPUs and 8,319,504,384 bytes of memory; Compose was 5.6.1, Java 21.0.7, Maven 3.10.0, Node 26.0.0, and Python 3.9.6. The [data index](j3-outbox-stages-2026-11-18/index.json) links both sanitized reports and per-request NDJSON files. Credential manifests are excluded. Reports include timestamps, exact producer commands, snapshots, percentiles, database and Kafka counters, and Docker resource samples.

## Results

| Repeat | Accepted / scheduled | Rejected / skipped | Accepted p95 / p99 | Terminal deliveries at final sample | Delivery lag p95 / p99 | Full drain within 120 s |
|---|---:|---:|---:|---:|---:|---|
| 1 | 1,000 / 1,000 | 0 / 0 | 395 / 1,197 ms | 3,000 / 3,000 | 87.3 / 91.7 s | No |
| 2 | 993 / 1,000 | 7 / 0 | 3,192 / 4,282 ms | 2,447 / 2,979 | 102.7 / 112.9 s | No |

All seven rejections in repeat 2 were HTTP 503; neither run had transport errors. Both missed delivery lag p95 ≤ 5 s, p99 ≤ 30 s, and full drain ≤ 120 s. Repeat 1 missed accepted latency p99 ≤ 1,000 ms. Repeat 2 missed accepted latency p95 ≤ 500 ms and p99 ≤ 1,000 ms. The 993 accepted events in repeat 2 met the 99% admission ratio, but its latency and drain still failed. These short runs are diagnostic controls, not the full-duration HLD §13 normal profile.

| Repeat | Final delivery-ready pending / published | Final analytics-fact pending / published | Oldest pending outbox age | Final analytics Kafka committed / end / lag | Analytics insert count / total duration |
|---|---:|---:|---:|---:|---:|
| 1 | 0 / 3,330 | 0 / 9,660 | 0 s | 7,982 / 10,660 / 2,678 | 8,543 / 103.9 s |
| 2 | 471 / 2,779 | 4,209 / 4,207 | 105.3 s | Final group positions unavailable | 4,058 / 115.4 s |

The analytics insert timer recorded no errors in either run. Its count is a service-side insert operation count, not a count of distinct facts or terminal deliveries. The delivery publisher's published counter was 12,990 and 7,404 at the respective final samples; it spans both message types. The SQL published totals and counter values differ slightly in repeat 2 because probes are sequential. The final analytics ingest-age gauges were 97 and 136 seconds. Repeat 2's final Kafka group command returned no position rows, while service metrics still showed insert activity and a 1,410 maximum consumer-lag gauge. Earlier repeat 2 samples showed analytics group lag increasing from 1,254 to 1,346. The missing final offset prevents a precise final Kafka backlog figure for repeat 2.

During four consecutive drain intervals in repeat 1, sampled publisher rates were about 127–138 messages/s and analytics insert rates about 61–71 operations/s; delivery outbox pending fell from 3,908 to 1,271 while analytics Kafka lag rose. In repeat 2's middle drain intervals, publisher rates were about 63–70 messages/s and analytics insert rates about 33–41 operations/s; outbox pending stayed above 4,900. These are counter differences divided by elapsed snapshot time, not isolated service capacity measurements. The later repeat 2 interval fell to about 30 and 15 per second. A shared host and expensive sequential probes are plausible contributors to the variance, but these runs cannot assign a causal share. Docker Desktop became unavailable only after the runner exited, was restarted once for verification, and no third load run was made.

## Next task

After review and merge, use these per-stage measurements to test a bounded publisher or analytics consumer change against an unchanged 8/8 control, with independent repeats and complete end-to-end criteria. Keep the 8/8 worker defaults. Full-duration 5,000/10,000/15,000 accepted-events/s profiles still need documented distributed resources, and production partition counts and concurrency caps remain unverified.

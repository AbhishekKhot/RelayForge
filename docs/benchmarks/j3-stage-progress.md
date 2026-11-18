# J3 delivery and analytics stage progress, 2026-11-19

**Finding:** At the checked-in 8-handler, 8-connection, one-fact analytics defaults, two fresh 50 events/s controlled runs cleared delivery work at about 30 terminal deliveries/s after arrivals stopped. Delivery-ready publication was faster in one run and about the same rate in the other. Analytics-fact publication outpaced Kafka consumption during both drains, so analytics lag cleared last or remained at the 120-second limit. The runs missed delivery-lag thresholds and do not establish distributed capacity.

## Method

The source base was the [PR #58](https://github.com/AbhishekKhot/RelayForge/pull/58) merge at `cfe43f5`. The only tracked behavior change adds `outbox_by_type` to each progress snapshot's delivery SQL. It records pending, published, in-flight, and oldest pending age for `delivery.ready.v1` and `analytics.delivery-facts.v1`. Existing progress samples already record terminal deliveries, delivery work, and Kafka committed/end offsets. The detailed before/after snapshots and `pipeline_drained` decision are unchanged. PostgreSQL's aggregate [`FILTER`](https://www.postgresql.org/docs/17/sql-expressions.html) limits each count to its status, and Kafka's [consumer-group position output](https://kafka.apache.org/39/operations/basic-kafka-operations/) supplies committed offsets and lag.

Each run used a fresh detached Compose project and isolated volumes, with 15 organizations, three matching endpoints each, 1 KiB request bodies, a 200 ms fixed receiver delay, 10% deterministic retryable responses, a 128 platform claim cap, and 1,000 maximum client requests in flight. The producer scheduled 50 events/s for 20 seconds, followed by at most 120 seconds of drain observation. The ignored environment set `DELIVERY_CONSUMER_WORKERS=8`, `DELIVERY_DATABASE_POOL_SIZE=8`, `RELAYFORGE_ANALYTICS_MAX_POLL_RECORDS=1`, and `RELAYFORGE_ANALYTICS_FACT_BATCH_SIZE=1`. This is the checked-in default configuration. No Maven build or other RelayForge benchmark overlapped the runs.

The host was an Apple M4 MacBook Air with 24 GB RAM. Docker Desktop 29.9.2 allocated 10 CPUs and 8,319,504,384 bytes of memory. Other host workloads were present. [The data index](j3-stage-progress-2026-11-19/index.json) links both sanitized JSON reports and per-request NDJSON; credential manifests are excluded. Reproduce with those settings in an ignored env file:

```sh
python3 load-tests/run_diagnostics.py --env-file .env.stage --only controlled
```

## End-to-end results

| Repeat | Accepted / scheduled | Rejections / skips | Accepted p95 / p99 | Terminal deliveries | Delivery lag p95 / p99 | Final analytics Kafka lag | Full drain ≤ 120 s |
|---|---:|---:|---:|---:|---:|---:|---|
| 1 | 1,000 / 1,000 | 0 / 0 | 840 / 1,502 ms | 3,000 / 3,000 | 81.5 / 86.1 s | 0 | Yes, 104.9 s |
| 2 | 995 / 1,000 | 5 / 0 | 1,165 / 1,910 ms | 2,985 / 2,985 | 84.7 / 89.4 s | 7 after the detailed final sample | No |

Both runs missed delivery lag p95 ≤ 5 s and p99 ≤ 30 s. Repeat 1 also missed accepted latency p95 ≤ 500 ms and p99 ≤ 1,000 ms. Repeat 2 had five HTTP 503 rejections and missed the accepted latency and drain thresholds. At the last progress sample, its analytics Kafka group still had 142 records of lag; the subsequent detailed final snapshot showed 7, so a zero-lag drain was not observed within the cap. Both runs had zero delivery work and zero pending delivery outbox rows at their final snapshots.

## Stage progress

| Repeat | Delivery-ready first observed with zero pending | Worker work first observed at zero | Analytics-fact outbox first observed with zero pending | Analytics Kafka first observed with zero lag |
|---|---:|---:|---:|---:|
| 1 | 40.0 s | 78.8 s | 46.5 s | 105.4 s |
| 2 | 78.8 s | 78.8 s | 78.8 s | Not observed by 123.4 s |

Times are measured from the first post-producer progress snapshot. A zero pending count can be followed by newly created rows, so these are first observations, not continuous-empty durations. At the instant delivery work first reached zero, analytics Kafka lag was still 2,539 and 3,993 records in the respective runs; its maximum sampled lag was 3,673 and 4,291. This separates worker completion from analytics consumption. The delivery-ready outbox cleared well before worker work in repeat 1, while in repeat 2 they cleared together. These samples do not prove the publisher was or was not the limiting stage in every interval.

| Rate after producer stop | Repeat 1 | Repeat 2 |
|---|---:|---:|
| Terminal deliveries until work first reached zero | 30.5/s | 30.3/s |
| Delivery-ready published rows until first zero pending | 53.1/s | 31.2/s |
| Analytics-fact published rows until first zero pending | 133.9/s | 106.3/s |
| Analytics Kafka committed offsets through last progress sample | 90.5/s | 77.9/s |

Each rate is the counter difference from the post-producer snapshot divided by elapsed snapshot wall time to the specified endpoint; these windows have different lengths. Publication counts include retry-related messages. The Kafka position is a consumer-group offset count, not a distinct fact count. Adjacent measurements are sequential and may be a few seconds apart. [The detailed final snapshot](j3-stage-progress-2026-11-19/index.json) recorded mean handler/outbound time of 226/202 ms in repeat 1 and 237/203 ms in repeat 2. About 200 ms of each attempt was spent in the configured receiver/HTTP call. The collector's median progress snapshot wall time was 1.54/2.05 s, and no snapshot probe failed.

The outbox publication and analytics consumer rates changed materially between the two repeats on this shared host. The roughly 30/s terminal worker rate repeated, but this experiment did not change worker concurrency or isolate CPU, database, Kafka, and HTTP resources. No production partition count, connection pool size, or delivery concurrency cap follows from these local results. Full-duration HLD §13 normal, peak, and headroom profiles remain unrun on distributed resources.

The progress behavior test failed before implementation and passed afterward. Fourteen Python benchmark tests, detached Compose configuration, one bounded runner with two fresh repeats, and Java 21 `TZ=UTC mvn -B -q verify` passed locally.

## Next task

On documented distributed resources, repeat stage measurements with independent load generators and service capacity. Use bounded, independently repeated evidence before changing delivery concurrency or publication settings. Treat the 5,000/10,000/15,000 accepted-events/s HLD figures as unverified goals.

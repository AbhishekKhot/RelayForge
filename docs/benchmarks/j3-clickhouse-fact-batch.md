# J3 bounded ClickHouse fact batching, 2026-11-18

**Finding:** Batching reduced ClickHouse fact lookup and insert calls by roughly half in two fresh controlled runs, but neither run met the J3 acceptance, delivery-lag, and drain thresholds. The analytics fact batch remains opt-in; the default is one fact per request. These single-host results do not establish production capacity.

## Method

The source base was the [PR #56](https://github.com/AbhishekKhot/RelayForge/pull/56) merge at `f95043d`, with the bounded fact-batch change in this PR. The consumer can group up to 32 valid records from one Kafka partition into one duplicate lookup and one JSONEachRow insert. Invalid envelopes split a group and are quarantined; a failed group is replayed from its first offset. The store checks existing fact versions and conflicting rows before writing. The feature is configured with `RELAYFORGE_ANALYTICS_FACT_BATCH_SIZE`, default `1`, and its cap cannot exceed `RELAYFORGE_ANALYTICS_MAX_POLL_RECORDS`.

Each setting had two independent fresh detached Compose projects and isolated volumes. The controlled profile scheduled 50 events/s for 20 seconds (1,000 events), with 15 organizations, three matching endpoints per organization, 1 KiB request bodies, a 200 ms receiver delay, 10% deterministic retryable receiver responses, and at most 120 seconds of post-producer drain. Both settings used 8 delivery handlers, an 8-connection worker pool, a 32-record analytics Kafka poll, and the same host. The only intentional setting difference was fact batch size `1` versus `32`. The runner removed each stack and its own volumes. No benchmark, Maven build, or other RelayForge Compose stack overlapped a run.

The host was an Apple M4 MacBook Air with 24 GB RAM. Docker Desktop 29.9.2 allocated 10 CPUs and 8,319,504,384 bytes of memory. [The data index](j3-fact-batch-2026-11-18/index.json) links the four JSON reports and per-request NDJSON files; credential manifests are excluded. Run the same controlled runner with an ignored `.env.benchmark`, setting `RELAYFORGE_ANALYTICS_MAX_POLL_RECORDS=32` and changing `RELAYFORGE_ANALYTICS_FACT_BATCH_SIZE` between `1` and `32`:

```sh
python3 load-tests/run_diagnostics.py --env-file .env.benchmark --only controlled
```

## Results

| Setting / repeat | Accepted / scheduled | Accepted p95 / p99 | Terminal deliveries | Delivery lag p95 / p99 | Final delivery outbox | Final analytics Kafka lag | Full drain |
|---|---:|---:|---:|---:|---:|---:|---:|
| One fact / 1 | 1,000 / 1,000 | 513 / 719 ms | 3,000 | 94.3 / 108.0 s | 0 | 0 | 112.5 s |
| One fact / 2 | 990 / 1,000 | 2,676 / 3,543 ms | 2,970 | 132.9 / 148.5 s | 1,250 | 474 | Did not drain |
| Up to 32 / 1 | 950 / 1,000 | 3,449 / 3,955 ms | 2,850 | 102.1 / 117.5 s | 0 | 0 | 112.7 s |
| Up to 32 / 2 | 1,000 / 1,000 | 1,694 / 2,443 ms | 3,000 | 105.6 / 125.1 s | 0 | 0 | 125.1 s |

The one-fact repeat 2 had ten HTTP 503 rejections, and batch repeat 1 had fifty. There were no generator skips. The 125.1-second drain in batch repeat 2 exceeded the 120-second goal because a final sequential collector snapshot crossed the deadline. Even the best run missed accepted-event p95 ≤ 500 ms and delivery lag p95 ≤ 5 s / p99 ≤ 30 s. The repeated settings varied substantially, so the observed admission and latency differences cannot be attributed confidently to the fact batch alone.

| Setting / repeat | Fact success counter | Lookup calls / total | Insert calls / total | Mean facts per lookup |
|---|---:|---:|---:|---:|
| One fact / 1 | 10,658 | 10,658 / 68.8 s | 10,658 / 56.5 s | 1.00 |
| One fact / 2 | 9,433 | 9,436 / 82.2 s | 9,436 / 69.0 s | ~1.00 |
| Up to 32 / 1 | 10,126 | 4,733 / 36.5 s | 4,733 / 33.0 s | 2.14 |
| Up to 32 / 2 | 10,660 | 5,166 / 40.0 s | 5,166 / 35.3 s | 2.06 |

Fact success counts include processed duplicates; stage calls include errors. Timer totals are cumulative per process, not independent wall time, and pipeline stages overlap. Actual batches averaged only about two facts because this controlled arrival pattern often supplied fewer than 32 contiguous records from the same partition. The lower ClickHouse call count is a real local efficiency gain, but the four runs provide no repeatable end-to-end gain. The collector itself takes several seconds per sequential snapshot and adds shared-host load.

Java 21 `TZ=UTC mvn -B -q clean verify`, 13 Python benchmark tests, and Compose configuration validation passed. Integration tests cover new fact batches, duplicates, conflicting versions, invalid-envelope quarantine, batch failure replay, and committing a successful prefix before a later failed group.

## Next task

Keep the default at one fact. Use the stage timers and a lower-overhead collector on distributed resources to isolate delivery publication and analytics throughput, then run full-duration HLD §13 normal, peak, and headroom profiles. The 5,000/10,000/15,000 accepted-events/s targets and production partition or concurrency choices remain unverified.

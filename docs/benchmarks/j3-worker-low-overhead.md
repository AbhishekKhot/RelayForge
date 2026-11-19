# J3 worker concurrency with low-cost collection, 2026-11-19

**Finding:** On the available single host, raising delivery handlers from 8 to 16 while keeping the PostgreSQL pool at 8 regressed the controlled profile in two independent fresh repeats. Terminal delivery throughput fell, accepted latency and delivery lag rose, and neither 16-handler run drained the entire pipeline within 120 seconds. Keep the checked-in 8/8 defaults. These local runs do not select a production concurrency cap or establish HLD §13 capacity.

## Method

The source base was the [PR #60](https://github.com/AbhishekKhot/RelayForge/pull/60) merge at `4a40e47`; no tracked service or collector code changed for this experiment. The two ignored env files differed only in `DELIVERY_CONSUMER_WORKERS=8` versus `16`. Both used `DELIVERY_DATABASE_POOL_SIZE=8`, `DELIVERY_PLATFORM_CONCURRENCY_LIMIT=128`, `RELAYFORGE_ANALYTICS_MAX_POLL_RECORDS=1`, and `RELAYFORGE_ANALYTICS_FACT_BATCH_SIZE=1`. The one-record analytics path and 8/8 worker settings are the current defaults. The lower-cost J3 progress collector recorded delivery work, outbox state by message type, and Kafka offsets during load and drain, with detailed snapshots before and after.

Each setting had two fresh detached Compose projects and isolated volumes. Every run scheduled 50 events/s for 20 seconds (1,000 events), with 15 organizations, three matching endpoints per organization, 1 KiB request bodies, a 200 ms fixed receiver delay, 10% deterministic retryable receiver responses, at most 1,000 client requests in flight, and at most 120 seconds of post-producer drain. No Maven build or other RelayForge benchmark overlapped the runs. The runner removed its own stack and volumes after each repeat. The host was an Apple M4 MacBook Air with 24 GB RAM; Docker Desktop 29.9.2 allocated 10 CPUs and 8,319,504,384 bytes of memory. Other host workloads were present and not controlled.

The [data index](j3-worker-low-overhead-2026-11-19/index.json) links the four sanitized JSON reports and per-request NDJSON. Credential manifests are excluded. Reproduce by setting the ignored env values above and running each command once; the runner performs two fresh repeats per setting:

```sh
python3 load-tests/run_diagnostics.py --env-file .env.worker8 --only controlled
python3 load-tests/run_diagnostics.py --env-file .env.worker16 --only controlled
```

## Results

| Handlers / pool | Repeat | Accepted / scheduled | Rejected / skipped | Accepted p95 / p99 | Terminal deliveries | Delivery lag p95 / p99 | Final analytics Kafka lag | Full drain ≤ 120 s |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| 8 / 8 | 1 | 1,000 / 1,000 | 0 / 0 | 955 / 1,653 ms | 3,000 / 3,000 | 84.7 / 89.3 s | 0 | Yes, 112.8 s |
| 8 / 8 | 2 | 1,000 / 1,000 | 0 / 0 | 1,700 / 2,614 ms | 3,000 / 3,000 | 83.5 / 88.3 s | 979 | No |
| 16 / 8 | 1 | 1,000 / 1,000 | 0 / 0 | 1,881 / 2,667 ms | 3,000 / 3,000 | 107.6 / 119.3 s | 2,961 | No |
| 16 / 8 | 2 | 998 / 1,000 | 2 / 0 | 3,800 / 4,871 ms | 2,994 / 2,994 | 92.7 / 102.9 s | 1,707 | No |

Both 16/8 repeats missed accepted-event p95 ≤ 500 ms, p99 ≤ 1,000 ms, delivery lag p95 ≤ 5 s, p99 ≤ 30 s, and full drain ≤ 120 s. Repeat 2's two rejections were HTTP 503. All four producers exited successfully and skipped no scheduled arrivals. Every final detailed snapshot showed zero delivery outbox rows pending, while the analytics Kafka group remained behind in three runs. [Kafka's group offset output](https://kafka.apache.org/41/operations/basic-kafka-operations/) defines that lag as the gap between committed and log-end offsets. No snapshot probe failed.

| Handlers / pool | Repeat | Terminal deliveries/s after producer stop | Mean handler / outbound call | Median progress snapshot wall time |
|---|---:|---:|---:|---:|
| 8 / 8 | 1 | 30.0 | 236 / 203 ms | 1.56 s |
| 8 / 8 | 2 | 31.3 | 229 / 202 ms | 2.24 s |
| 16 / 8 | 1 | 21.6 | 266 / 205 ms | 2.60 s |
| 16 / 8 | 2 | 24.6 | 252 / 204 ms | 2.10 s |

Terminal rates divide the increase in succeeded plus dead-lettered deliveries by the elapsed time from the first post-producer snapshot to the first snapshot with zero delivery work. The handler and outbound means divide each final timer's cumulative total time by count; stages overlap and are not separate wall times. Outbound time stayed near the configured 200 ms receiver delay, but the mean handler time increased with 16 handlers. The detailed snapshots did not measure Hikari pending connections during the busy interval, so they cannot show whether the eight-connection pool briefly saturated. [HikariCP documents](https://github.com/brettwooldridge/HikariCP#maximumPoolSize) that its maximum pool size is a connection cap and borrowers wait when no idle connection is available. The 16/8 regression could reflect shared CPU, database, Kafka, scheduler, pool, or measurement pressure; this experiment does not isolate their causal shares. A previous 16/12 experiment with a heavier collector also failed to improve the profile, but it is not a matched comparison to these runs.

The four detached runner repeats and Compose configuration checks passed. The release targets remain unmet even at 8/8. Full-duration HLD §13 5,000/10,000/15,000 accepted-events/s profiles still require documented distributed resources, and no production worker, pool, shard, or partition sizing follows from this single-host diagnostic.

## Next task

Use a provisioned distributed environment to measure busy-interval handler occupancy, Hikari waiters, PostgreSQL load, Kafka publication/consumption, and end-to-end acceptance before revisiting concurrency. Retain the 8/8 defaults and treat any missed profile as a finding.

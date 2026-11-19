# J3 benchmark plan and local diagnostic boundary

The [distributed pre-prod deployment and measurement plan](j3-distributed-preprod-plan.md) lists the environment decisions, implementation gaps, provisioning gates, and evidence needed before these full profiles can run. No distributed environment exists yet.

HLD §13 sets **planning goals**, not demonstrated capacity: 5,000 accepted events/s normal, 10,000 peak, and 15,000 headroom, with average fan-out 3 and 1.1 outbound attempts per delivery. The corresponding estimated attempt rates are 16,500, 33,000, and 49,500/s. Final capacity tests need distributed producers, application replicas, write shards with synchronous standbys, Kafka brokers, and ClickHouse on documented resources. A single-host Compose result cannot establish those production rates.

## Proposed full-profile acceptance contract

| Profile | Arrival rate | Duration per repeat | Repeats |
|---|---:|---:|---:|
| Normal | 5,000 events/s | 30 minutes | 3 |
| Peak | 10,000 events/s | 5 minutes | 3 |
| Headroom | 15,000 events/s | 2 minutes | 3 |

Use 100 organizations and 10,000 active endpoints where the final environment can support them, with an average of three matching subscriptions per event. Use 1 KiB serialized event bodies, a documented receiver latency distribution centered near 200 ms, and enough retryable receiver failures to measure an attempt ratio near 1.1. Record the **actual** distribution, fan-out, and attempt ratio. Configure at least one hot-tenant slice rather than assuming uniform traffic. Warm the system for two minutes before each run and collect at least five minutes of bounded post-run drain data. Reset to an equivalent starting state before a repeat; compare percentiles and throughput across repeats.

The following thresholds are chosen for J3 evaluation; they are not HLD guarantees: at least 99% of scheduled arrivals return durable `202` within the run window, no generator-skipped arrivals, accepted-event p95 ≤ 500 ms and p99 ≤ 1,000 ms, delivery completion lag from event acceptance p95 ≤ 5 s and p99 ≤ 30 s, and delivery backlog drains within 120 s after arrivals stop. Record HTTP non-202 status counts, transport errors, dead letters, retries, Kafka lag, PostgreSQL locks/connections/WAL/disk, Redis hits/misses/memory, and ClickHouse fact rows/query latency. A run is **unmet** if a threshold fails or its required measurement is unavailable. Do not average a failed repeat into a passing claim.

The [PostgreSQL 17 statistics views](https://www.postgresql.org/docs/17/monitoring-stats.html), [Kafka consumer group lag](https://kafka.apache.org/41/operations/basic-kafka-operations/), [Redis `INFO`](https://redis.io/docs/latest/commands/info/), [ClickHouse query logging](https://clickhouse.com/blog/clickhouse-debugging-issues-with-system-tables), and [Docker container stats](https://docs.docker.com/reference/cli/docker/container/stats/) define the sampled counters. `load-tests/benchmark.py` records bounded snapshots and raw request outcomes; counters are cumulative, so reports use before/after differences. Delivery lag joins delivery state to its event acceptance timestamp. Samples may themselves perturb a small local system; report that limitation.

## Available machine and diagnostic runs

The available MacBook Air has an Apple M4 with 10 cores and 24 GB host RAM. Docker Desktop provides 10 CPUs and 8,319,504,384 bytes of memory. This Compose stack has one Kafka broker, one logical ingestion and delivery shard, a single ClickHouse node, and an operator-set delivery claim cap. Other Docker workloads may share the host. For local diagnostics, use 15 organizations, three subscribed endpoints each, 1 KiB bodies, fixed 200 ms receiver latency, 10% retryable receiver responses, and a 128-claim platform cap. These choices deliberately bound machine pressure; 15 organizations and 45 endpoints are below the HLD account/endpoint planning population. Use independent fresh stacks for **two diagnostic repeats** at each target arrival rate, with bounded windows of 5 s normal, 3 s peak, and 2 s headroom, 1,000 client in-flight requests, and a 120 s drain ceiling. The local probes exercise the target **arrival rates** for short windows and report accepted rates, skipped arrivals, backlog, and bottlenecks. They do not satisfy the full-profile durations or topology above.

Save sanitized per-request NDJSON, sampled JSON, command lines, image and tool versions, resource limits, and observed errors for every diagnostic. Keep API keys, credentials, and connection URLs in ignored env and manifest files only. If the local stack cannot drain, stop the test after the configured drain limit and record the remaining backlog. Do not run another profile against a dirty backlog when comparing independent repeats.

The [100-organization, 10,000-endpoint population preflight](j3-population-preflight-2026-11-20.md) verified three matching subscriptions per organization on a local stack. Its slow configuration-projection catch-up is a separate finding and does not satisfy any full-duration profile.

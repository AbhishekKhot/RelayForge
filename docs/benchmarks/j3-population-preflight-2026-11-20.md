# J3 endpoint-population preflight, 2026-11-20

This local preflight tested workload setup at the HLD planning population. It did not run a J3 normal, peak, or headroom profile and is not capacity evidence for a distributed release.

## Environment and bounded run

- Source: `develop` at `b37b471` (PRs #63–#65 merged).
- One isolated detached Compose project with the ingestion HA and load-receiver overlays; Docker Desktop 29.9.2, Compose 5.6.1, 10 allocated CPUs, and 8,319,504,384 allocated memory bytes. Other Docker workloads shared the host. Node.js 26.0.0 ran the setup tool.
- The receiver was configured for 10 ms latency and 25% retryable failures. The setup credential, API-key manifest, connection URLs, and local env file remained ignored. The project and its own volumes were removed after verification.
- Compose startup had a 1,200 s process limit and a 600 s health wait; setup had a 900 s process limit. The projection observation had a 600 s limit and stopped at that limit; subsequent read-only checks observed eventual completion.

The setup used `node load-tests/setup.mjs --tenants 100 --fanout 3 --endpoints-per-tenant 100 --run-id j3-population-full` with the ignored `.env.loadtest` and a new ignored credential-manifest path. Its output reported 100 organizations and 10,000 active endpoints. The setup command ran from 18:18:35 to 18:19:04 UTC, about 29 seconds.

## Observed state

| Check | Result |
|---|---:|
| Control-plane organizations | 100 |
| Control-plane active endpoints and subscriptions | 10,000 each |
| Control outbox pending | 0 |
| Delivery active subscription projection | 10,000 |
| Subscriptions matching the produced event type | 300, or three per organization |
| Subscriptions for the unused event type | 9,700 |
| Ready projection organizations | 100 |
| Router configuration consumer lag at final check | 0 |

The 600 s projection wait ended with 6,100 active subscriptions and 183 matching subscriptions. The `projection_state.updated_at` values for the 100 organizations ranged from 18:18:36.799 to 18:36:18.573 UTC, a **1,062 s span**. Projection therefore lagged well behind the short setup command on this single-host stack. The router configuration consumer currently has one Kafka partition and one record in flight; this is a candidate bottleneck, not an isolated causal measurement.

With all 10,000 subscriptions projected, one 1 KiB event returned `202` with no skipped arrival. It created one routing record and three deliveries; all three reached `SUCCEEDED`, and the ingestion and delivery outboxes had zero pending rows at the final check. The [sanitized producer record](j3-population-preflight-2026-11-20.ndjson) contains the single request timing. The local `202` latency of 1,017.596 ms is an observation for one request, not a percentile or a throughput result.

## Next evidence

Investigate configuration-projection throughput with a bounded repeated comparison before changing consumer limits. Run the three full-duration J3 profiles on documented distributed resources with service-side and producer evidence. Do not use this setup check to select production shard counts, Kafka partitions, or delivery concurrency caps.

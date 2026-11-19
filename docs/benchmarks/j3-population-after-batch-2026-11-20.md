# J3 full population projection recheck after PR #67

This is a bounded single-host recheck of configuration projection at the HLD planning population. It does not measure accepted-event throughput or complete a J3 release profile.

## Environment and method

- Source: `develop` at `01b8ea7` (PR #67 merged), using the router's default configuration intake cap of 64 and one ordered handler worker.
- One isolated, detached HA Compose project with the load receiver. Docker Desktop 29.9.2 allocated 10 CPUs and 8,319,504,384 bytes of memory; Compose 5.6.1 and Node.js 26.0.0 were used. Other workloads shared the host.
- Setup command: `node load-tests/setup.mjs --tenants 100 --fanout 3 --endpoints-per-tenant 100 --run-id j3-population-after-batch-20261120`, using an ignored local env file and credential manifest.
- Compose boot had a 1,200-second process limit and 600-second health wait. Setup had a 900-second limit. Projection and Kafka drain observation stopped at 600 seconds from setup start and polled the delivery projection every two seconds. The isolated project and its own volumes were removed after the checks; the credential manifest was deleted.

## Observations

| Check | Result |
|---|---:|
| Setup duration | 30.46 s |
| All 10,000 active subscriptions projected, from setup start | 45.37 s |
| Router configuration consumer lag zero, from setup start | 46.47 s |
| Span from first to last `projection_state.updated_at` | 43.71 s |
| Control-plane organizations | 100 |
| Control-plane active endpoints and subscriptions | 10,000 each |
| Control outbox pending | 0 |
| Delivery projection organizations ready | 100 |
| Delivery active subscriptions | 10,000 |
| Matching / idle active subscriptions | 300 / 9,700 |
| Final router configuration consumer lag | 0 |

The earlier [one-record preflight](j3-population-preflight-2026-11-20.md) took 1,062 seconds from first to last projection-state update at the same population. This recheck observed 43.71 seconds, roughly 24 times shorter on that measure. The runs were on separate fresh stacks at different times, so this is a local improvement observation rather than an isolated capacity claim. The [smaller repeated comparison](j3-config-projection-batch-2026-11-20.md) changed only the router intake cap and supports the same direction of effect.

No event producer or delivery-lag profile ran in this recheck. Full-duration normal, peak, and headroom profiles still require documented distributed resources and service-side evidence. The 5,000/10,000/15,000 accepted-events/s design goals, production partition counts, and delivery concurrency caps remain unverified.

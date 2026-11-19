# J3 configuration projection intake comparison, 2026-11-20

This is a bounded single-host comparison of router configuration intake. It is not a full J3 profile or production capacity result.

## Setup

- Source: `develop` at `e1e5345` (PR #66 merged), with the configurable router intake change on this branch.
- One isolated detached HA Compose project with the load receiver. Docker Desktop 29.9.2 had 10 CPUs and 8,319,504,384 memory bytes allocated; other workloads shared the host. The project and only its own volumes were removed afterward.
- The router configuration topic had one partition. Its handler stayed on one worker in every run, preserving message order. The only changed setting was `ROUTER_CONFIG_MAX_IN_FLIGHT`, which also sets Kafka `max.poll.records` for this consumer.
- Each repeat created five new organizations with 100 active endpoints and subscriptions per organization, of which three per organization matched the load event type. The setup command was `node load-tests/setup.mjs --tenants 5 --fanout 3 --endpoints-per-tenant 100` with a unique run ID and ignored credential manifest per repeat. Repeats were sequential in the same stack with disjoint organizations.
- Elapsed time starts immediately before setup and ends on the first two-second poll that sees all 500 new active subscriptions projected. Setup had a 180-second command limit; each projection wait had a 240-second limit. Compose boot and router recreation had explicit time limits. Results therefore include setup time and up to two seconds of polling resolution.

| Config max in flight | Repeat 1 | Repeat 2 | Projected active subscriptions |
|---:|---:|---:|---:|
| 1 | 45 s | 53 s | 500 each |
| 64 | 4 s | 4 s | 500 each |

After all four repeats, the delivery database contained 2,000 active projected subscriptions and all 20 organization projection states were ready. Java 21 `mvn -B -q verify` passed, including a test that holds the first queued message and verifies later messages are handled in order and committed only after completion. The default is now 64 records in flight with **one** handler worker; accepted configuration revisions are still processed serially. The setting is capped at 256 and can be overridden for measurement or recovery.

The first attempt stopped before provisioning because its short run ID produced an idempotency key below the control plane's 16-character minimum and returned HTTP 400. The corrected second attempt completed all four comparisons. No benchmark or verification process remains running.

## Limits and next evidence

The repeats shared one host and stack, and the batch runs followed the baseline runs, so cache warming may contribute to the difference. We did not record Kafka lag between repeats; duplicate messages from an earlier repeat could have affected the next repeat's elapsed time. This comparison establishes a strong local projection bottleneck signal, not a distributed throughput guarantee. Recheck projection behavior and complete J3 normal, peak, and headroom profiles on documented distributed resources before selecting production partition counts or concurrency limits. The 5,000/10,000/15,000 accepted-events/s design goals remain unverified.

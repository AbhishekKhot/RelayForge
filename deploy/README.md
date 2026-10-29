# Deployment plan

Each independently runnable role has its own image and Docker functional test target. `relayforge-contracts`, `relayforge-runtime`, and `relayforge-api-support` are libraries and have no images. `Dockerfile.service` builds a separate runnable image for a selected module; `compose.yaml` and the `test-*` targets are D0 implementation tasks.

| Maven module | Runtime image | Compose service | Docker test target |
|---|---|---|---|
| `relayforge-control-plane` | `relayforge/control-plane` | `control-plane` | `test-control-plane` |
| `relayforge-control-publisher` | `relayforge/control-publisher` | `control-publisher` | `test-control-publisher` |
| `relayforge-ingestion` | `relayforge/ingestion` | `ingestion` | `test-ingestion` |
| `relayforge-ingestion-publisher` | `relayforge/ingestion-publisher` | `ingestion-publisher` | `test-ingestion-publisher` |
| `relayforge-router` | `relayforge/router` | `router` | `test-router` |
| `relayforge-delivery-api` | `relayforge/delivery-api` | `delivery-api` | `test-delivery-api` |
| `relayforge-delivery-worker` | `relayforge/delivery-worker` | `delivery-worker` | `test-delivery-worker` |
| `relayforge-delivery-publisher` | `relayforge/delivery-publisher` | `delivery-publisher` | `test-delivery-publisher` |
| `relayforge-retry-scheduler` | `relayforge/retry-scheduler` | `retry-scheduler` | `test-retry-scheduler` |
| `relayforge-analytics-consumer` | `relayforge/analytics-consumer` | `analytics-consumer` | `test-analytics-consumer` |
| `relayforge-analytics-api` | `relayforge/analytics-api` | `analytics-api` | `test-analytics-api` |

Local Compose also runs PostgreSQL 17 with separate control, ingestion, and delivery databases, Kafka, Redis, ClickHouse, and a controllable `webhook-simulator` image. It runs one logical ingestion and one logical delivery shard for functional tests. A separate HA test profile adds a synchronous standby and failure controller to test the `202` contract, fencing, and restore. A runtime image contains only the application and Java runtime; `test-*` targets use a build/test image and Dockerized dependencies. Build every role's image and run its functional, integration, security, and failure tests before its checkpoint is marked complete.

The final capacity environment must distribute load generators, API/worker replicas, PostgreSQL shard primaries and standbys, Kafka brokers, and ClickHouse across documented resources. Do not infer the 5,000/10,000/15,000 accepted-events/s profiles from a single-host Compose run. Record hardware, replica counts, per-service limits, shard map, Kafka partitions, payload/fan-out mix, receiver latency, and observed bottlenecks. Select physical shard count and image replica counts only from those measurements. Publish immutable image digests for production rollouts; local `:dev` tags are disposable.

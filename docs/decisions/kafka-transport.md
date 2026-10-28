# Kafka transport contract

Kafka transports committed work; PostgreSQL remains the authority. Every producer publishes from its owner's outbox with a stable `message_id`, `acks=all`, and idempotence enabled. Consumers commit offsets only after their local database or ClickHouse write succeeds. Duplicate messages remain possible and must be harmless.

| Topic | Key | Consumer | Recovery authority |
|---|---|---|---|
| `relayforge.config.changed.v1` | `organization_id` | One projector group per delivery shard | Control-plane configuration snapshot and revision |
| `relayforge.event.accepted.v1` | `event_id` | Global router group; write the locator's delivery shard | Ingestion event/outbox and delivery routing progress |
| `relayforge.delivery.ready.v1` | `delivery_id` | Global worker group; claim the locator's delivery shard | `delivery_work` and day-partitioned delivery row |
| `relayforge.analytics.delivery-facts.v1` | `fact_id` | Analytics consumer | Delivery attempts/state and delivery outbox |

The first local Compose environment uses one broker, one partition per topic, and replication factor 1. Production uses at least three brokers, replication factor 3, `min.insync.replicas=2`, and `acks=all`; the exact partition count is set by the final throughput and skew tests. A producer error leaves the outbox row retryable. Do not change the configuration topic's partition count in place while it carries per-organization ordered revisions; use an explicit topic migration and projection catch-up. [Kafka topic replication settings](https://kafka.apache.org/43/generated/topic_config.html).

The initial topic retention is **24 hours**, `cleanup.policy=delete`, with a measured storage budget and alerts before any consumer approaches that lag. Published outbox rows are eligible for bounded cleanup after 48 hours only when the relevant consumer has passed their broker offsets and recovery checks pass. If a consumer outage exceeds retention, repair control projections from the control-plane snapshot, recover delivery-ready work from `delivery_work`, and backfill accepted-event routing or analytics from the authoritative PostgreSQL cohorts in bounded batches. Alert and throttle admission if that repair cannot drain within the tested backlog envelope. The 24/48-hour windows are operational defaults to validate in Docker failure tests and the final load suite, not a claim that Kafka can hold the stated target rate on an unspecified cluster.

Each message includes schema version, organization ID, correlation ID, and immutable locator or fact identity. Reject unknown versions to a bounded quarantine path; do not expose payloads, credentials, or tokens in transport diagnostics. No ordering is promised for different events or deliveries. A 50,000 virtual-user test is not a throughput target without measured accepted events/s and outbound attempts/s.

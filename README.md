# RelayForge

RelayForge accepts customer events and delivers them as webhooks to registered endpoints. Customers configure accounts, API keys, endpoints, and subscriptions; producers submit events with an idempotency key. RelayForge durably accepts each event before routing it to matching endpoints, retrying eligible failures, and recording delivery history and analytics.

The repository has runnable Spring Boot role scaffolds and a shared image build. Business endpoints, database migrations, Compose, and end-to-end tests are still implementation tasks. Follow the [implementation checklist](RelayForge_Implementation_Checklist.md) for the build sequence.

## High-level design

![RelayForge system context](docs/diagrams/system-context.svg)

![Durable event acceptance](docs/diagrams/event-acceptance.svg)

![Routing, delivery, and retries](docs/diagrams/routing-delivery.svg)

![Service ownership and analytics flow](docs/diagrams/logical-architecture.svg)

## Implementation stack

| Component | Planned role |
|---|---|
| Java 21, Spring Boot, Maven | Service and worker applications; Maven reactor for the modules |
| PostgreSQL, Flyway | Authoritative service-owned data, local outboxes, and migrations |
| Kafka | Asynchronous event, delivery-work, and analytics-fact transport |
| Redis | Short-lived quota, token-cache, and coordination state |
| ClickHouse | Eventually consistent delivery analytics |
| Docker Compose | Local functional and integration environment once implemented |

PostgreSQL holds accepted events and active delivery state; Kafka messages may repeat, so consumers must be idempotent. The [HLD](RelayForge_High_Level_Design.md), [LLD](RelayForge_Low_Level_Design.md), [physical schema plan](docs/schema/README.md), and [image matrix](deploy/README.md) contain the detailed contracts.

## Design load estimates

Planning assumes **100 customer accounts**, **10,000 active endpoints**, average fan-out of **3 deliveries per accepted event**, and **1.1 outbound attempts per delivery**. Estimated outbound attempts/s = accepted events/s × 3 × 1.1.

| Profile | Accepted events/s | Estimated outbound attempts/s | Intended use |
|---|---:|---:|---|
| Normal | 5,000 | 16,500 | Sustained planning load |
| Peak | 10,000 | 33,000 | Burst planning load |
| Headroom | 15,000 | 49,500 | Peak plus 5,000 events/s |

These are design assumptions, not measured capacity. At a continuous normal rate for 90 days, the plan implies about **38.88 billion events**, **116.64 billion deliveries**, and **128.30 billion outbound attempts**, before auxiliary records. Event and delivery history become unavailable after 90 days; daily partition removal normally has a planned physical grace of up to 25 hours, with blocked drops treated as incidents. Account and configuration records are not age-purged. Shard count, hardware, burst duration, and the final throughput claim depend on load tests after functional implementation.

## Local setup

Install **JDK 21** and **Maven**, then run from the repository root:

```bash
java -version
mvn -version
mvn validate
mvn test
```

`mvn validate` checks the reactor layout and Java Checkstyle rules; `.editorconfig` defines four-space indentation. `mvn test` runs the bootstrap and PostgreSQL 17.11 Testcontainers migration tests, so Docker must be running. Each deployable module has a Spring Boot entry point. To run the control-plane bootstrap locally, use `mvn package` followed by `java -jar relayforge-control-plane/target/relayforge-control-plane-0.1.0-SNAPSHOT.jar`, then inspect `http://localhost:8080/health/live`. Database clients for application queries and dependency-backed readiness belong to later checkpoints; readiness currently reflects process startup only.

The control-plane, ingestion, and delivery-api images apply their own Flyway migration at startup when `SPRING_FLYWAY_URL`, `SPRING_FLYWAY_USER`, and `SPRING_FLYWAY_PASSWORD` are set. Point each role at a separate PostgreSQL 17 database; ingestion and delivery use one database per shard. Bootstrap creates yesterday, today, and seven future UTC-day partitions. The migration tests verify a second apply leaves the schema history unchanged, and exercise local uniqueness, ownership, and partition boundaries. See [schema ownership and retention](docs/schema/README.md) for the remaining operational jobs.

Build a role image with `docker build -f deploy/Dockerfile.service --build-arg MODULE=relayforge-control-plane -t relayforge/control-plane:dev .`. Replace the module and image name for another role. `compose.yaml` and dependency-backed readiness belong to the D0 checkpoint after A2; `docker compose up` is not supported yet. The [checklist](RelayForge_Implementation_Checklist.md) tracks that gate.

## Navigate the codebase

```text
RelayForge/
├── pom.xml                         Maven reactor
├── relayforge-runtime/              Validated runtime settings
├── relayforge-api-support/          Shared HTTP errors, request IDs, health
├── relayforge-db-migration/         Conditional owner Flyway startup
├── relayforge-schema-tests/         PostgreSQL 17 migration checks
├── relayforge-contracts/            Cross-service message contracts
├── relayforge-control-plane/        Accounts, keys, endpoints, subscriptions
├── relayforge-control-publisher/    Configuration outbox publisher
├── relayforge-ingestion/            Event acceptance and idempotency
├── relayforge-ingestion-publisher/  Accepted-event outbox publisher
├── relayforge-router/               Subscription matching and fan-out
├── relayforge-delivery-api/         Delivery queries and replay
├── relayforge-delivery-worker/      Webhook execution and attempts
├── relayforge-delivery-publisher/   Delivery and analytics outbox publisher
├── relayforge-retry-scheduler/      Due-work and lease recovery
├── relayforge-analytics-consumer/   Delivery facts into ClickHouse
├── relayforge-analytics-api/        Analytics queries
├── docs/diagrams/                  Architecture SVGs
├── docs/decisions/                 Reviewed design decisions
├── deploy/                         Image build and planned Compose setup
└── load-tests/                     Planned capacity workloads
```

Each service module has its own `pom.xml` and `src/main/java` directory; tests live under `src/test/java`. Start with the [HLD](RelayForge_High_Level_Design.md) for architecture, the [LLD](RelayForge_Low_Level_Design.md) for contracts and schemas, and the [checklist](RelayForge_Implementation_Checklist.md) for implementation order and verification gates.

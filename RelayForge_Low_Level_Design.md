# RelayForge --- Low-Level Design (LLD)

**Status:** Implementation design awaiting physical-schema review\
**Aligned documents:** RelayForge High-Level Design;
RelayForge Implementation Checklist\
**Primary stack:** Java 21, Spring Boot, PostgreSQL, Kafka, Redis,
ClickHouse\
**Design principle:** PostgreSQL is authoritative; Kafka transports
work; ClickHouse serves eventually consistent analytics.

**Capacity scope:** Section 5.1–5.11 contains logical field examples;
the service-owned physical SQL in [`docs/schema/`](docs/schema/README.md)
is the migration source. HLD §13 defines the load and lifecycle contract.
Review and validate the physical schema gate before Milestone A.

------------------------------------------------------------------------

## 1. Purpose and scope

This document translates the RelayForge HLD into implementation-level
design. It defines module boundaries, Java packages, database
structures, API contracts, message envelopes, transaction boundaries,
delivery state transitions, authentication handling, retry behavior,
security controls, and testing requirements.

This is the implementation contract for RelayForge. Implementation and benchmark completion are tracked separately in `RelayForge_Implementation_Checklist.md`.

### 1.1 In scope

-   Organization creation and API-key lifecycle.
-   Endpoint registration, endpoint authentication configuration, and
    event-type subscriptions.
-   Event ingestion with idempotency and transactional outbox.
-   Event-type-based fan-out routing.
-   Durable delivery scheduling and execution.
-   Static authentication and token-exchange/OAuth flows.
-   Retry scheduling, expired-lease recovery, dead-lettering, and
    replay.
-   Delivery history and ClickHouse analytics.
-   Security, observability, and automated tests.

### 1.2 Out of scope for the initial release

-   Active-active multi-region deployment.
-   Exactly-once external side effects.
-   Guaranteed ordering across deliveries.
-   Arbitrary customer-supplied scripts for authentication or request
    transformation.
-   Dedicated infrastructure per organization.
-   Per-endpoint custom code execution.
-   Complex visual workflow definitions.

### 1.3 Normative language

-   **MUST**: required for correctness or security.
-   **SHOULD**: recommended; deviations require a documented reason.
-   **MAY**: optional or deferred.

------------------------------------------------------------------------

## 2. Key design decisions

  --------------------------------------------------------------------------------
  Decision                Initial choice                   Reason
  ----------------------- -------------------------------- -----------------------
  Source of truth         PostgreSQL                       Transactions for
                                                           accepted events,
                                                           routing, delivery
                                                           state, attempts, and
                                                           outbox records

  Async transport         Kafka                            Buffering and
                                                           independent consumer
                                                           scaling; duplicate
                                                           delivery is expected

  DB-to-Kafka consistency Transactional outbox             Avoids dual-write loss
                                                           between PostgreSQL and
                                                           Kafka

  Routing                 `organization_id + event_type`   One event may fan out
                          to active subscriptions          to multiple endpoints

  Event acceptance        `202 Accepted` after event +     Producer latency is
                          outbox commit                    independent of
                                                           destination
                                                           availability

  Delivery guarantee      At-least-once HTTP attempts      A crash after receiver
                                                           success can cause a
                                                           duplicate

  Receiver deduplication  Stable `delivery_id` header      Gives receivers a key
                                                           for deduplicating
                                                           repeated requests

  Retry scheduling        PostgreSQL `next_attempt_at`     Durable, inspectable
                          plus indexed scanner             retry state; no
                                                           sleeping worker threads

  Delivery ownership      PostgreSQL lease with            Crash recovery and
                          token/version                    stale-worker protection

  Token cache             Redis with TTL jitter,           Avoids repeated auth
                          local single-flight, and         calls and refresh
                          per-key distributed lock         stampedes; never
                                                           authoritative for
                                                           delivery state

  Analytics               Dedicated analytics outbox →     Analytics failure must
                          Kafka → ClickHouse               not block delivery

  Subscription timing     Snapshot matching                Predictable fan-out;
                          endpoint/subscription IDs when   later config changes do
                          routing occurs                   not silently alter
                                                           already-created
                                                           deliveries

  Replay                  Create a new delivery generation Preserves original
                          linked to original               delivery history and
                                                           makes replay auditable

  Initial deployables     Modular Spring Boot app plus     Avoids premature
                          separately scalable worker       microservice
                          processes                        proliferation

  Ordering                No ordering guarantee            Keep initial
                                                           implementation simpler
                                                           and increase throughput
  --------------------------------------------------------------------------------

### 2.1 Important identifier distinction

-   `organization_id`: identifies the owning organization.
-   `endpoint_id`: identifies a registered destination configuration.
-   `event_type`: routing key, e.g. `payment.completed`.
-   `event_id`: identifies one event accepted by RelayForge.
-   `subscription_id`: identifies the association between an endpoint
    and an event type.
-   `delivery_id`: identifies one logical delivery generation to one
    endpoint.
-   `attempt_id`: identifies one HTTP delivery attempt.
-   `idempotency_key`: client-supplied key for deduplicating
    event-publish requests.
-   `outbox_id`: identifies one durable message-to-be-published record.

An endpoint registration returns `endpoint_id` and its registered
`event_types`. It does **not** return an event ID, because no event has
been published yet.

------------------------------------------------------------------------

## 3. Suggested repository and package structure

The Maven reactor matches the four ownership boundaries. Runtime roles
use separate images, while roles inside one boundary share that owner's
database and contracts. Each owner keeps Flyway migrations in its own
module; the SQL templates in `docs/schema/` are the physical reference.

``` text
RelayForge/
├── relayforge-control-plane/
├── relayforge-control-publisher/
├── relayforge-ingestion/
├── relayforge-ingestion-publisher/
├── relayforge-router/
├── relayforge-delivery-api/
├── relayforge-delivery-worker/
├── relayforge-delivery-publisher/
├── relayforge-retry-scheduler/
├── relayforge-analytics-consumer/
├── relayforge-analytics-api/
├── relayforge-contracts/
├── docs/schema/
├── deploy/
└── load-tests/
```

### 3.1 Dependency direction

`controller → application service → domain / repository interfaces → persistence adapters`.

-   Controllers parse HTTP requests and map responses; they must not
    contain business workflows.
-   Application services orchestrate use cases and transaction
    boundaries.
-   Domain classes represent state and invariants; they should not
    depend on Spring MVC, Kafka clients, or ClickHouse clients.
-   Persistence adapters implement repository interfaces using Spring
    Data JDBC/JPA or explicit JDBC. For lease claims and high-contention
    SQL, prefer explicit SQL/JDBC.
-   Kafka consumers call application services and acknowledge/commit
    offsets only after the database transaction has completed.
-   External HTTP calls must not run inside an open database
    transaction.

### 3.2 Core application services

  -----------------------------------------------------------------------
  Class                               Responsibility
  ----------------------------------- -----------------------------------
  `OrganizationService`               Create organization and issue API
                                      key

  `ApiKeyService`                     Generate, hash, verify, rotate, and
                                      revoke API keys

  `EndpointRegistrationService`       Validate and persist endpoint
                                      configuration

  `EndpointSecurityValidator`         Validate URL, DNS/IP targets,
                                      redirect policy, and request limits

  `SubscriptionService`               Create, update, deactivate and
                                      query event-type subscriptions

  `EventIngestionService`             Validate event, enforce
                                      idempotency, persist event + outbox

  `EventRoutingService`               Resolve active subscriptions and
                                      create delivery records

  `DeliveryClaimService`              Atomically claim eligible work and
                                      establish a lease

  `DeliveryExecutionService`          Execute one delivery lifecycle and
                                      record result

  `OutboundRequestBuilder`            Compose URL, headers, body, auth
                                      and signature

  `AuthenticationStrategyResolver`    Select static auth, OAuth, or
                                      token-exchange strategy

  `TokenProvider`                     Resolve or acquire an access token

  `TokenCache`                        Cache token data by endpoint/auth
                                      configuration version

  `RetryPolicy`                       Classify errors and compute retry
                                      time

  `RetrySchedulingService`            Re-enqueue due work through outbox

  `LeaseRecoveryService`              Recover work with expired leases

  `ReplayService`                     Create replay generation and audit
                                      record

  `OutboxPublisher`                   Publish committed outbox records to
                                      Kafka

  `AnalyticsFactFactory`              Build analytics facts from state
                                      changes

  `AnalyticsQueryService`             Query ClickHouse with organization
                                      scope and limits
  -----------------------------------------------------------------------

------------------------------------------------------------------------

## 4. Domain model

### 4.1 Organization and API credential

An organization owns endpoint configuration, event types, events,
deliveries, quotas, and analytics.

API keys should use a prefix for lookup and a cryptographically secure
random secret. Store a strong one-way hash of the secret, not the
plaintext key. The plaintext key is returned only at creation/rotation
time.

Suggested API-key format:

``` text
rf_live_<public-key-id>_<random-secret>
```

-   `public-key-id` is a non-secret lookup identifier.
-   `random-secret` is high entropy.
-   Persist `key_id`, `organization_id`, `secret_hash`, `scopes`,
    `created_at`, `last_used_at`, `expires_at`, and `revoked_at`.
-   Use a keyed HMAC or a password-hashing algorithm appropriate for
    high-entropy API keys; compare digests in constant time.
-   Never log the presented key or include it in error messages.

### 4.2 Endpoint and subscription

An endpoint stores destination configuration and an authentication
strategy. A subscription links an endpoint to an event type.

An endpoint may subscribe to multiple event types. An event type may
have multiple endpoints. This is a many-to-many relation through
`subscriptions`.

### 4.3 Event, delivery, attempt

-   An `Event` represents a durable accepted business event.
-   A `Delivery` represents the logical attempt lifecycle for one
    event-to-endpoint pair (or replay generation).
-   A `DeliveryAttempt` represents one actual outbound HTTP attempt.
-   Token acquisition attempts should be recorded separately from
    webhook HTTP attempts, or represented as typed diagnostic attempts,
    so token endpoint failures are not misreported as target webhook
    responses.

### 4.4 Outbox record

An outbox row represents a committed intention to publish a message to
Kafka. It is written in the same PostgreSQL transaction as the state
change that requires it.

------------------------------------------------------------------------

## 5. PostgreSQL schema

Use PostgreSQL migrations (Flyway recommended). UUIDs are primary
identifiers; externally visible IDs may be UUIDs serialized as strings
with prefixes for readability. If prefixed IDs are adopted, store UUIDs
internally and map the prefix at the API boundary.

The following schema lists logical fields and example constraints from the
initial single-database model. Use the PostgreSQL 17 templates in
[`docs/schema/`](docs/schema/README.md) for physical keys, local foreign
keys, and partition creation. These examples are not Flyway migrations.
Section 5.13 defines ownership and access contracts.

### 5.1 `organizations`

``` sql
CREATE TABLE organizations (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(160) NOT NULL,
    status          VARCHAR(24) NOT NULL DEFAULT 'ACTIVE'
                    CHECK (status IN ('ACTIVE', 'SUSPENDED', 'DELETED')),
    plan_code       VARCHAR(40) NOT NULL DEFAULT 'FREE',
    quota_config    JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### 5.2 `api_keys`

``` sql
CREATE TABLE api_keys (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id),
    key_id          VARCHAR(32) NOT NULL UNIQUE,
    secret_hash     BYTEA NOT NULL,
    scopes          TEXT[] NOT NULL DEFAULT ARRAY['events:write'],
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at    TIMESTAMPTZ,
    expires_at      TIMESTAMPTZ,
    revoked_at      TIMESTAMPTZ
);
CREATE INDEX idx_api_keys_org ON api_keys(organization_id);
```

Only active, unexpired keys can authenticate. For very frequent API-key
checks, cache verification metadata briefly, but revocation must have a
bounded propagation delay and must not rely on unbounded stale cache.

### 5.3 `users`

``` sql
CREATE TABLE users (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id),
    email           VARCHAR(320) NOT NULL,
    display_name    VARCHAR(160) NOT NULL,
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    status          VARCHAR(24) NOT NULL DEFAULT 'ACTIVE'
                    CHECK (status IN ('ACTIVE', 'DISABLED')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (organization_id, email)
);
CREATE INDEX idx_users_org_status ON users(organization_id, status);
```

The initial user endpoint is a trusted provisioning API. This table does
not by itself define password storage, sessions, SSO, or a complete
identity lifecycle; those remain outside the initial delivery pipeline
scope. Normalize email consistently before enforcing uniqueness.

### 5.4 `endpoints`

``` sql
CREATE TABLE endpoints (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id     UUID NOT NULL REFERENCES organizations(id),
    name                VARCHAR(160) NOT NULL,
    base_url            TEXT NOT NULL,
    path                TEXT NOT NULL,
    query_params        JSONB NOT NULL DEFAULT '{}'::jsonb,
    headers             JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    auth_type           VARCHAR(40) NOT NULL,
    auth_config_ciphertext BYTEA,
    auth_secret_ref     TEXT,
    signing_secret_ciphertext BYTEA,
    status              VARCHAR(24) NOT NULL DEFAULT 'ACTIVE'
                        CHECK (status IN ('ACTIVE', 'PAUSED', 'DISABLED')),
    timeout_ms          INTEGER NOT NULL DEFAULT 10000
                        CHECK (timeout_ms BETWEEN 100 AND 60000),
    max_concurrency     INTEGER NOT NULL DEFAULT 10
                        CHECK (max_concurrency BETWEEN 1 AND 1000),
    max_attempts        SMALLINT NOT NULL DEFAULT 10
                        CHECK (max_attempts BETWEEN 1 AND 20),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    version             BIGINT NOT NULL DEFAULT 1,
    UNIQUE (id, organization_id)
);
CREATE INDEX idx_endpoints_org_status ON endpoints(organization_id, status);
```

Implementation notes: - `headers` must not contain plaintext secrets.
Secret-valued headers belong in the encrypted authentication
configuration. - `auth_config_ciphertext` stores versioned ciphertext
produced by application-level envelope encryption. The plaintext data
encryption key (DEK) is never stored; it is wrapped by a key-encryption
key (KEK) held in the deployment's trusted key-management configuration.
Persist the wrapped DEK, ciphertext, algorithm/key version, and nonce as
needed by the selected authenticated-encryption implementation. Support
key rotation by rewrapping DEKs or re-encrypting configuration. Never log
plaintext configuration or cryptographic material. - `auth_secret_ref`
is not used in the initial envelope-encryption path. - Do not allow untrusted
users to override reserved RelayForge headers (`X-RelayForge-*`, `Host`,
`Content-Length`, `Connection`, etc.). - Endpoint URL validation must be
repeated at delivery time to defend against DNS changes; do not rely
only on registration-time checks.

### 5.5 `subscriptions`

``` sql
CREATE TABLE subscriptions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id),
    endpoint_id     UUID NOT NULL,
    event_type      VARCHAR(200) NOT NULL,
    filter_config   JSONB,
    status          VARCHAR(24) NOT NULL DEFAULT 'ACTIVE'
                    CHECK (status IN ('ACTIVE', 'PAUSED', 'DELETED')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    version         BIGINT NOT NULL DEFAULT 1,
    UNIQUE (organization_id, endpoint_id, event_type),
    UNIQUE (id, organization_id),
    FOREIGN KEY (endpoint_id, organization_id)
        REFERENCES endpoints(id, organization_id)
);
CREATE INDEX idx_subscriptions_route
    ON subscriptions(organization_id, event_type, status, endpoint_id);
CREATE INDEX idx_subscriptions_endpoint
    ON subscriptions(organization_id, endpoint_id, status);
```

The organization ID must match the endpoint's organization. Enforce this
in the application transaction and, if desired, strengthen the schema
with composite unique/FK constraints.

### 5.6 `events`

``` sql
CREATE TABLE events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    event_type      VARCHAR(200) NOT NULL,
    occurred_at     TIMESTAMPTZ,
    schema_version  INTEGER NOT NULL DEFAULT 1,
    payload         JSONB NOT NULL,
    payload_hash    BYTEA NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    status          VARCHAR(24) NOT NULL DEFAULT 'ACCEPTED'
                    CHECK (status = 'ACCEPTED'),
    accepted_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (organization_id, idempotency_key),
    UNIQUE (id, organization_id)
);
CREATE INDEX idx_events_org_created
    ON events(organization_id, created_at DESC, id DESC);
CREATE INDEX idx_events_route_lookup
    ON events(organization_id, event_type, accepted_at);
```

The idempotency policy is organization-scoped. If an existing key has
the same canonical request hash, return the original event ID. If the
body differs, return `409 IDEMPOTENCY_KEY_REUSED`. Canonicalization must
be stable and documented; do not hash arbitrary JSON serialization whose
key order may vary.

Canonical request hash format 1 is SHA-256 of RFC 8785 JSON
canonicalization over an object containing `event_type`, resolved
`schema_version` (default `1`), `data`, and `occurred_at` only when the
client supplies it. Normalize a supplied timestamp to UTC with at most
microsecond precision before canonicalization. Exclude server-generated
`accepted_at`, authorization, and the idempotency key. Reject duplicate
JSON property names, invalid Unicode, and numeric values outside the
I-JSON range required by RFC 8785. Store `request_hash_version = 1` in
the shard-local registry. For example,
`{"data":{"a":1,"b":2},"event_type":"order.created","schema_version":1}`
hashes to
`8b3bea11df1868a5726ddca10480859e250e30242b8a0b867c87bea11453e21d`.
Use `io.github.erdtman:java-json-canonicalization:1.1` behind a small
adapter and verify it against RFC vectors before use.
[RFC 8785](https://www.rfc-editor.org/rfc/rfc8785),
[Java canonicalization artifact](https://central.sonatype.com/artifact/io.github.erdtman/java-json-canonicalization/1.1).

Initially, payloads are stored in PostgreSQL JSONB with a strict size cap.
Published event history and related delivery diagnostics expire 90 days
after RelayForge's `accepted_at`, regardless of client `occurred_at`.
Idempotency keys remain unique for 90 days in a separate shard-local
registry committed atomically with event and outbox. Reuse after expiry
creates a new event. The event table's unique constraint above is only
the logical contract; it cannot enforce a cross-day window on daily
partitions. Consider object storage only if measured payload size and
retrieval requirements justify it; any copy needs a matching lifecycle.

### 5.7 `deliveries`

``` sql
CREATE TABLE deliveries (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id     UUID NOT NULL,
    event_id            UUID NOT NULL,
    endpoint_id         UUID NOT NULL,
    subscription_id     UUID,
    generation          INTEGER NOT NULL DEFAULT 1,
    status              VARCHAR(32) NOT NULL DEFAULT 'PENDING'
                        CHECK (status IN (
                            'PENDING', 'ENQUEUED', 'IN_PROGRESS',
                            'RETRY_SCHEDULED', 'SUCCEEDED', 'DEAD_LETTERED',
                            'CANCELLED'
                        )),
    attempt_count       INTEGER NOT NULL DEFAULT 0,
    claim_count         INTEGER NOT NULL DEFAULT 0,
    next_attempt_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner          VARCHAR(160),
    lease_token          UUID,
    lease_expires_at    TIMESTAMPTZ,
    last_http_status    INTEGER,
    last_error_code     VARCHAR(80),
    last_error_message  VARCHAR(500),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at        TIMESTAMPTZ,
    version             BIGINT NOT NULL DEFAULT 1,
    UNIQUE (event_id, endpoint_id, generation)
);
CREATE INDEX idx_deliveries_due
    ON deliveries(status, next_attempt_at, created_at)
    WHERE status IN ('PENDING', 'RETRY_SCHEDULED', 'ENQUEUED');
CREATE INDEX idx_deliveries_org_created
    ON deliveries(organization_id, created_at DESC, id DESC);
CREATE INDEX idx_deliveries_event
    ON deliveries(organization_id, event_id);
CREATE INDEX idx_deliveries_endpoint_status
    ON deliveries(organization_id, endpoint_id, status, created_at DESC);
CREATE INDEX idx_deliveries_expired_lease
    ON deliveries(lease_expires_at)
    WHERE status = 'IN_PROGRESS';
```

`attempt_count` counts outbound HTTP requests begun, including a controlled
refresh-and-retry after a destination 401. `claim_count` counts worker
ownership claims, including claims that fail before destination HTTP.
Authentication attempts are counted only in `authentication_attempts`.
Enforce `max_attempts` against `attempt_count` immediately before each
HTTP send; a lease recovery may create another claim without spending
an HTTP attempt. The unique constraint ensures one logical delivery per
event/endpoint/generation. Replay increments `generation` and creates a
new delivery linked to the original via `replay_requests`.

### 5.8 `delivery_attempts`

``` sql
CREATE TABLE delivery_attempts (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    delivery_id         UUID NOT NULL REFERENCES deliveries(id),
    attempt_number      INTEGER NOT NULL,
    started_at          TIMESTAMPTZ NOT NULL,
    finished_at         TIMESTAMPTZ,
    outcome             VARCHAR(32) NOT NULL,
    http_status         INTEGER,
    duration_ms         INTEGER,
    error_code          VARCHAR(80),
    error_detail        VARCHAR(1000),
    response_excerpt    TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (delivery_id, attempt_number)
);
CREATE INDEX idx_delivery_attempts_delivery
    ON delivery_attempts(delivery_id, attempt_number);
```

Persist only sanitized, bounded response excerpts. Do not persist
response headers or bodies that may contain tokens, PII, or credentials
without an explicit policy. Consider storing `error_detail` as an
internal safe code plus a short sanitized message.

### 5.9 `authentication_attempts`

``` sql
CREATE TABLE authentication_attempts (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    endpoint_id         UUID NOT NULL,
    delivery_id         UUID REFERENCES deliveries(id),
    attempt_number      INTEGER NOT NULL,
    started_at          TIMESTAMPTZ NOT NULL,
    finished_at         TIMESTAMPTZ,
    outcome             VARCHAR(32) NOT NULL,
    http_status         INTEGER,
    duration_ms         INTEGER,
    error_code          VARCHAR(80),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_auth_attempts_delivery
    ON authentication_attempts(delivery_id, started_at DESC);
```

This table records authentication-stage failures separately from webhook
delivery attempts. Never store the token, password, client secret, or
raw token response here.

### 5.10 `outbox`

``` sql
CREATE TABLE outbox (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type      VARCHAR(60) NOT NULL,
    aggregate_id        UUID NOT NULL,
    message_type        VARCHAR(100) NOT NULL,
    topic               VARCHAR(200) NOT NULL,
    partition_key       VARCHAR(200) NOT NULL,
    payload             JSONB NOT NULL,
    status              VARCHAR(20) NOT NULL DEFAULT 'PENDING'
                        CHECK (status IN ('PENDING', 'IN_FLIGHT', 'PUBLISHED')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    available_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_by          VARCHAR(160),
    claim_token         UUID,
    claim_expires_at    TIMESTAMPTZ,
    published_at        TIMESTAMPTZ,
    publish_attempts    INTEGER NOT NULL DEFAULT 0,
    last_error_code     VARCHAR(80)
);
CREATE INDEX idx_outbox_pending
    ON outbox(available_at, created_at)
    WHERE status IN ('PENDING', 'IN_FLIGHT');
CREATE INDEX idx_outbox_aggregate
    ON outbox(aggregate_type, aggregate_id, created_at);
```

Remove published outbox rows on a separately configured operational
schedule after broker acknowledgment and recovery checks. Never delete
a pending or unpublished row merely because it is old; alert, recover,
or explicitly terminate it before removing its event cohort. Test a
publisher crash and replay through the recovery window.

### 5.11 `replay_requests` and `audit_records`

``` sql
CREATE TABLE replay_requests (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id     UUID NOT NULL,
    original_delivery_id UUID NOT NULL REFERENCES deliveries(id),
    replay_delivery_id  UUID NOT NULL REFERENCES deliveries(id),
    requested_by_key_id UUID,
    reason              VARCHAR(500),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (replay_delivery_id)
);

CREATE TABLE audit_records (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID REFERENCES organizations(id),
    actor_key_id    UUID REFERENCES api_keys(id),
    action          VARCHAR(100) NOT NULL,
    resource_type   VARCHAR(60) NOT NULL,
    resource_id     UUID,
    request_id      VARCHAR(100),
    details         JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_audit_org_created
    ON audit_records(organization_id, created_at DESC);
```

Audit `organization.created`, `api_key.rotated/revoked`,
`endpoint.created/updated/disabled`,
`subscription.created/updated/disabled`, and `delivery.replayed`. Never
include secrets in `details`.

### 5.12 Schema rules and migrations

-   Every organization-owned query MUST include the authenticated
    `organization_id`.
-   Enforce tenant ownership with composite keys and foreign keys only
    within an owning database. For cross-service references, validate the
    organization and resource at the API or message boundary, and persist
    the verified IDs and configuration version. Retain authorization checks.
-   Expire historical event cohorts after 90 days; never age-purge
    organizations, API keys, users, endpoints, or subscriptions. Keep
    account/configuration audit records under a separate policy. Check
    live work and references before detaching historical partitions;
    use the documented physical-deletion grace for partial cutoff days
    and bounded cleanup for published outbox.
-   Use Flyway migrations with forward-only schema changes in normal
    deployment.
-   Add indexes based on query plans and the final load tests; each index adds
    write and storage overhead.
-   Use `TIMESTAMPTZ` and UTC.
-   Use optimistic version columns for editable endpoint/subscription
    configuration.
-   Avoid using JSONB for high-cardinality fields used frequently in
    joins or predicates.
-   The PostgreSQL 17 physical templates define partition/shard keys,
    composite uniqueness, and local foreign-key scope. Size the physical
    topology using measured row sizes and final load tests.

### 5.13 Physical PostgreSQL design gate

The executable PostgreSQL 17 schema templates and retention/migration
order are in [`docs/schema/`](docs/schema/README.md). They supersede the
logical examples above for physical constraints and indexes. Gate review
must inspect those templates and the contracts below together.

#### Storage ownership and transaction boundaries

Each owner has its own database and migration history. Worker roles share
the owning service's database credentials, restricted to its tables. Local
Compose may place these databases on one PostgreSQL instance; that does not
permit cross-database joins, foreign keys, or transactions.

| Owner | Stored tables | Atomic writes and consumers |
|---|---|---|
| Control plane | `organizations`, `api_keys`, `users`, `endpoints`, `subscriptions`, `shard_bucket_map`, account/configuration `audit_records`, `control_outbox` | Provisioning, endpoint and subscription changes, and their configuration-change outbox records commit locally. Bucket-map changes use a fenced operational workflow. |
| Ingestion | `bucket_fence`, `idempotency_registry`, `events`, `ingestion_outbox` | Registry claim, event payload, and `event.accepted` outbox record commit on the selected ingestion shard before `202`. |
| Delivery | `routing_progress`, `routing_targets`, `deliveries`, `delivery_work`, `delivery_attempts`, `authentication_attempts`, `replay_requests`, delivery replay audit, `delivery_outbox`, `endpoint_projection`, `subscription_projection` | Router, worker, scheduler, replay, and delivery publisher commit state and messages on the selected delivery shard. |
| Analytics | ClickHouse delivery facts and query models | Kafka facts are deduplicated by their stable fact identity; no PostgreSQL transaction spans analytics. |

The generic `outbox` example in §5.10 is a template for the three local
outboxes, not a shared table. A service may publish only rows committed in
its own database. The delivery router never updates ingestion `events`;
`routing_progress` owns routing status, completion, and any cursor. An
event query composes ingestion acceptance with delivery progress through
bounded internal reads; a lagging delivery view is reported as pending.

#### Cross-service data contracts

Control-plane changes publish a versioned, secret-free configuration
snapshot from `control_outbox`. The router applies it idempotently to
durable `endpoint_projection` and `subscription_projection` tables keyed
by organization and resource ID; the router selects only active endpoints
and subscriptions. Each subscription projection row carries
`endpoint_id`, `event_type`, filter, status, and monotonically
increasing source version. Each endpoint/subscription change increments
`organizations.config_revision` and writes an outbox message containing
that organization revision in the same control-plane transaction. The
configuration topic is keyed by organization ID; `projection_state`
tracks the last contiguous revision, and any gap pauses that organization's
routing until a bounded snapshot repair catches up. Every delivery shard
that owns buckets for an organization maintains its own projection and
consumer offset; a new shard bootstraps a snapshot before accepting routes.
Apply a snapshot and advance `projection_state` in one local transaction;
commit its Kafka offset only after that transaction commits. Duplicate
or older revisions are ignored, and endpoint/subscription tombstones are
retained until all event cohorts that referenced them have expired.
Keep the configuration topic's partition count stable during normal
operation so per-organization ordering does not change unexpectedly. In
one delivery transaction, routing records
the selected `(subscription_id, endpoint_id, source_version)` rows in
`routing_targets` and a cursor in `routing_progress`. Later chunks use
those persisted targets; redelivery cannot silently change fan-out. A
gap, stale projection, or unavailable change stream pauses affected routing
and triggers bounded repair from a control-plane snapshot API. Projection
bootstrap and catch-up must complete before the router claims readiness.

The worker fetches the endpoint's current status and versioned delivery
configuration from a bounded, authenticated control-plane read immediately
before sending. It fails closed on an unavailable or disabled endpoint;
retryable read failures remain durable delivery work. Secrets never enter
Kafka or the subscription projection. Control-plane responses and caches
must be scoped by organization and have explicit size and timeout limits.

`event.accepted` carries organization ID, event ID, acceptance cohort,
exact `accepted_at`, ingestion locator version, event type, and payload
digest/reference. The ingestion service exposes an authenticated internal payload read by this
locator; it checks organization and event identity, returns the immutable
payload only while retained, and uses bounded response size and deadline.
The router retrieves it when filters need payload fields; the worker
retrieves it before constructing the outbound request. A
temporary read failure retries without spending an HTTP attempt; a
confirmed expired event is handled by the retention/replay policy. Kafka
and delivery rows do not hold a second authoritative payload copy.

Cross-service IDs are claims, not database foreign keys. On message
consumption, verify organization ID, locator/cohort, message version, and
resource ownership against the authoritative owner or an authenticated,
versioned projection. Reject mismatches to a bounded error path; never
silently create a delivery for an unverified endpoint or event. Local
composite keys enforce organization scope where both tables share a
database. No service writes another owner's tables, and no request holds a
database transaction across an internal read or Kafka publish.

| Table | Lifecycle and intended owner |
|---|---|
| `organizations`, `api_keys`, `users`, `endpoints`, `subscriptions`, `shard_bucket_map` | Control-plane configuration and routing metadata; no event-age purge |
| `events` | Ingestion history; 90 days from `accepted_at` |
| `deliveries`, `delivery_attempts`, `authentication_attempts`, `replay_requests` | Delivery history; expire with the source event's acceptance cohort |
| `delivery_work` | Active due/lease index; remove rows on terminal outcomes, never age-purge live work |
| `outbox` | Owner-local work; delete published rows after recovery checks, never pending rows by age |
| `audit_records` | Retain account/configuration actions separately; event-specific details follow event lifecycle |

Physical migrations MUST implement these contracts before Milestone A:

1. Give historical rows an immutable UTC `accepted_day` copied from the
   event; time-partition high-volume ingestion and delivery tables by
   day within their owning write shard. Pre-create partitions and remove
   fully expired cohorts only after active-work/reference checks. The
   partially expired day has a planned 25-hour physical-deletion grace;
   logical reads and replay stop at exactly 2,160 hours. Replay cannot
   extend the source event's
   retention window.
2. Route ingestion by stable hash of `(organization_id, idempotency_key)`
   to a shard. Commit the idempotency registry, event, and ingestion
   outbox together. A shard-local registry enforces the 90-day key
   window across event-day partitions; benchmark its bounded expiry
   separately. Do not rely on `UNIQUE (organization_id, idempotency_key)`
   on a time-partitioned events parent. Define a versioned locator so
   event/delivery lookups and rebalancing remain deterministic.
3. Route delivery records from the event locator; keep delivery and
   ingestion writes in their respective owning databases. Replace the
   example cross-service foreign keys with validated organization,
   event, endpoint, and cohort references at message/API boundaries.
   Within a database, composite keys and foreign keys must include
   partition columns where needed. Test tenant isolation and replay
   across day boundaries.
4. Give every production write shard a primary and standby, backups,
   monitored WAL/replication lag, and tested failover. Require
   synchronous standby WAL confirmation before ingestion returns `202`;
   return a retryable error if no synchronous standby is available.
   Standbys do not increase primary write capacity. Do not assign a
   shard count until write, disk, WAL, and tenant-skew measurements exist.
5. Keep any old pending/in-flight outbox and active delivery visible
   until recovered or explicitly terminated. A partition drop cannot
   silently discard work. Align Kafka, backups, logs, and optional
   payload copies with separately documented lifecycles. ClickHouse
   retention is unchanged by this PostgreSQL decision.

#### Ingestion shard routing and idempotency

Use 4,096 logical buckets to separate stable key placement from the
measured number of physical shards. For routing format 1, validate an
idempotency key of at most 255 UTF-8 bytes, reject control characters and
leading/trailing whitespace, and preserve its exact remaining bytes and
case. Compute `SHA-256(organization UUID bytes || two-byte big-endian key
length || key UTF-8 bytes)`; the first 12 digest bits are `bucket_id`
(`0..4095`). The UUID uses its network-order 16 bytes. The algorithm and
bucket count are protocol constants; a change requires a new routing
format and a migration, not a deployment setting. Example: organization
`00000000-0000-0000-0000-000000000001` with key `order-123` yields
bucket `3410`, and `order-124` yields `2152`. This spreads one hot
organization's **distinct** keys while keeping a retried key on one
bucket. It provides no event ordering guarantee.

A versioned `shard_bucket_map` in control-plane PostgreSQL assigns each
bucket a logical ingestion shard and delivery shard. Its epoch changes
commit with a local outbox record and are distributed to ingress and
workers; the map is not queried from control-plane PostgreSQL for every
event. Each shard ID
resolves to its current primary via service discovery, independently of
the map version. The selected bucket, map epoch, and both logical shard
IDs become immutable fields of the event locator. The external `evt_...`
ID encodes a versioned opaque locator containing those fields, UTC
`accepted_day`, and the event UUID; the internal message carries the same
fields explicitly. The delivery ID similarly carries its delivery shard,
acceptance day, and delivery UUID. Decoding an ID supplies a route, never
authorization: every read checks the authenticated organization against
the stored row. Old locators keep pointing to old data after a bucket
moves; logical shard IDs cannot be reused for a different data set.

On the selected ingestion shard, a non-time-partitioned
`idempotency_registry` has a unique `(organization_id, idempotency_key)`
key, an accepted timestamp, `expires_at = accepted_at + 90 days`, a
versioned canonical-request hash, and the original event locator. The
registry, event payload, and `ingestion_outbox` row commit in one local
transaction. Capture the database `clock_timestamp()` once after any
registry lock wait and use it for expiry comparison and `accepted_at`;
`now()` is fixed at transaction start and can be stale after a wait.
An unexpired matching key returns its original event;
an unexpired key with a different canonical request returns `409`.
At or after `expires_at`, replace the registry claim atomically and
create a new event. Use a row-level uniqueness conflict/lock so
concurrent requests cannot each create an event. Expired-row cleanup is
bounded, indexed by `expires_at`, and measured separately from event
partition removal. Registry expiry does not make an old active delivery
safe to purge. A unique constraint on daily event partitions cannot
enforce this cross-day window because PostgreSQL requires partition keys
in partitioned-table unique constraints.
[PostgreSQL partitioned uniqueness](https://www.postgresql.org/docs/current/ddl-partitioning.html),
[atomic conflict handling](https://www.postgresql.org/docs/current/sql-insert.html),
[transaction time functions](https://www.postgresql.org/docs/current/functions-datetime.html).

Each acceptance transaction locks its `bucket_fence` row `FOR SHARE` and
checks the expected map epoch and `ACTIVE` state before claiming a key.
Concurrent accepts can share that lock; a move updates the fence row and
waits for their transactions to finish. Benchmark the lock and WAL cost
at normal, peak, and headroom load.
[PostgreSQL row-lock behavior](https://www.postgresql.org/docs/current/explicit-locking.html).

To move a bucket, mark its old shard's acceptance fence draining and let
in-flight transactions finish; stale ingress instances must receive a
retryable error. Copy every unexpired registry row for the bucket to the
target and verify counts and hashes before publishing a higher map epoch.
The target then accepts new claims; the source rejects them. Event rows
and pending outbox work stay on their original shard and remain reachable
through stored locators. The source registry copy remains read-only until
expiry for recovery checks. An interrupted move keeps the source as owner
until the verified map cutover; after cutover, rollback requires the same
fenced procedure in reverse. A shard-local `bucket_fence` check inside
each acceptance transaction prevents stale cached maps from writing to
the old owner. The map publisher and shard fences must be tested together
before online moves are enabled; until then, use a fixed map and plan
capacity without an assumed shard count.

Admission limits and per-organization quotas protect against a hot tenant
whose rate exceeds its share of ingress, storage, or delivery capacity.
Track per-bucket rate and registry growth; a single repeatedly used key
serializes on its registry row. Bounded backpressure returns a retryable
response rather than allowing unbounded connection or outbox growth.
After a crash or uncertain commit, retry with the same key, recompute its
bucket, and read the registry on the current owner; locators preserve
access to the original event even if the bucket was moved.

#### PostgreSQL durability, failover, and backups

Every production control-plane database and ingestion/delivery write shard
has one writable primary and at least one physical standby in a separate
failure domain. Ingestion uses a named synchronous standby with
`synchronous_standby_names` set on the primary and `synchronous_commit = on`
for every event/idempotency/outbox transaction. PostgreSQL then waits for
the standby's WAL flush before reporting a successful commit. Do not use
`remote_write`, `local`, or `off` for this transaction; these do not meet
the acknowledged-event durability contract. Keep `fsync` enabled. Verify
the effective settings at startup and after failover, not only in a
configuration template. [PostgreSQL synchronous replication](https://www.postgresql.org/docs/current/warm-standby.html).

If the named synchronous standby is absent, stop accepting new events
before opening a transaction where possible. A standby can disappear
during commit, so bound the request deadline and return a retryable
service error if confirmation is unavailable. A timed-out or disconnected
commit has an **unknown outcome**, not a proven rollback: the producer
must retry with the same idempotency key, and the new primary must resolve
that key against its registry before creating another event. Never return
`202` from a pre-commit health check alone. A production failover target
must contain every acknowledged commit; otherwise failover is blocked
pending recovery. Simultaneous loss of primary and synchronous standby
remains a separate disaster-recovery scenario.

Use a failover controller that fences the old primary before promoting a
standby and switching the write endpoint. PostgreSQL provides promotion
but does not provide the complete failure-detection and client-routing
system. Reject writes while leadership is uncertain; drain or replace
stale application connections after the switch. Promote only a standby
whose durable WAL position covers the last acknowledged ingestion commit.
Rebuild the former primary as a standby before returning it to service.
With only one standby, ingestion stays unavailable after its promotion
until a new synchronous standby is ready. Do not assume an asynchronous
standby has all acknowledged transactions.
[PostgreSQL failover](https://www.postgresql.org/docs/current/warm-standby-failover.html).

Take regular physical base backups for **each** database/shard and
continuously archive WAL to storage outside that shard's failure domain.
Keep a complete base backup plus the WAL sequence needed for each promised
restore point, and retain timeline history across promotions. Backup
retention is a separately approved recovery policy, not the 90-day event
row lifecycle. Back up configuration and key-management material through
their own protected procedures; a database restore without usable keys
cannot recover encrypted endpoint settings. Exercise point-in-time restore
to an isolated cluster and verify event, registry, outbox, delivery, and
configuration consistency before calling a backup usable.
[PostgreSQL continuous archiving and PITR](https://www.postgresql.org/docs/current/continuous-archiving.html).

Monitor primary role, `pg_stat_replication` sync state and flush position,
WAL/archive failures, slot-retained WAL, backup age, restore success,
commit latency, and application acceptance errors per shard. Cap WAL
retention from replication slots and alert before disk exhaustion.
Record recovery time and any lost **unacknowledged** work in failover
drills; do not assign an untested RTO or fixed shard count. The local
single-instance Compose profile tests behavior, not production failover.
[PostgreSQL replication statistics](https://www.postgresql.org/docs/current/monitoring-stats.html).

------------------------------------------------------------------------

## 6. HTTP API contracts

Base path: `/v1`. All request/response bodies use JSON. All
authenticated API calls derive `organization_id` from the API key, never
from a trusted request-body field.

### 6.1 Common headers

Request: - `Authorization: Bearer rf_live_<key-id>_<secret>` -
`Content-Type: application/json` -
`X-Request-Id: <optional-correlation-id>` -
`Idempotency-Key: <required-for-event-publish>`

Response: - `X-Request-Id` - `Content-Type: application/json` -
`Retry-After` for throttling or temporary service unavailability where
applicable.

Common error format:

``` json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "The request is invalid.",
    "request_id": "req_...",
    "details": [
      {"field": "destination.base_url", "issue": "HTTPS is required."}
    ]
  }
}
```

Never return stack traces, secret values, internal network addresses, or
unbounded destination response bodies.

### 6.2 Create organization

`POST /v1/organizations` is a platform-provisioning operation, not an
open anonymous signup endpoint. It creates the organization and its
initial API credential. Access is restricted to a trusted provisioning
flow/operator credential. A user is associated with an organization
through `POST /v1/users`; that request includes an existing
`organization_id` and user metadata. User creation must verify that the
organization exists and that the caller is authorized to provision a
user for it.

Request:

``` json
{
  "name": "Acme Payments",
  "plan_code": "FREE",
  "quota_overrides": {
    "events_per_minute": 600,
    "max_active_endpoints": 25,
    "max_in_flight_deliveries": 100
  }
}
```

`201 Created`:

``` json
{
  "data": {
    "organization_id": "org_...",
    "name": "Acme Payments",
    "api_key": "rf_live_<key-id>_<secret>",
    "api_key_warning": "Shown once. Store it securely."
  },
  "request_id": "req_..."
}
```

Only the creation/rotation response returns plaintext key material.
Persist only the key ID and hash. If `quota_overrides` is omitted, apply
the defaults for the selected `plan_code`; if provided, validate each
value against platform-enforced maximums and persist it in
`organizations.quota_config`.

### 6.3 Create user

`POST /v1/users` is a trusted provisioning API for creating a user record
associated with an existing organization. It is not authenticated with a
normal organization API key; protect it with a separate provisioning
credential and strict authorization. The exact identity-provider and
user-login lifecycle is outside the initial delivery pipeline.

Request:

``` json
{
  "organization_id": "org_...",
  "email": "operator@example.com",
  "name": "Operations User",
  "metadata": {"team": "engineering"}
}
```

The service validates the organization and user metadata, creates the
user record, and writes an audit record in one transaction. Do not accept
an organization ID as authoritative from an untrusted caller.

### 6.4 Register endpoint

`POST /v1/endpoints` --- `201 Created`.

Request:

``` json
{
  "name": "Payments production",
  "destination": {
    "base_url": "https://api.customer.example",
    "path": "/v2/webhooks/payments",
    "query_params": {"source": "relayforge"},
    "headers": {"Content-Type": "application/json"},
    "metadata": {"environment": "production"}
  },
  "authentication": {
    "type": "TOKEN_EXCHANGE",
    "token_request": {
      "method": "POST",
      "url": "https://auth.customer.example/api/login",
      "headers": {"Content-Type": "application/json"},
      "body_fields": {
        "username": {"source": "secret", "name": "username"},
        "password": {"source": "secret", "name": "password"}
      }
    },
    "token_response": {
      "access_token_path": "$.data.accessToken",
      "expires_in_path": "$.data.expiresIn",
      "token_type": "Bearer"
    },
    "token_placement": {
      "type": "HEADER",
      "name": "Authorization",
      "prefix": "Bearer "
    },
    "secrets": {
      "username": "customer-user",
      "password": "secret"
    }
  },
  "event_types": ["payment.completed", "payment.failed"],
  "delivery_policy": {
    "timeout_ms": 10000,
    "max_attempts": 10,
    "max_concurrency": 10
  }
}
```

`201 Created`:

``` json
{
  "data": {
    "endpoint_id": "ep_...",
    "name": "Payments production",
    "status": "ACTIVE",
    "event_types": ["payment.completed", "payment.failed"],
    "created_at": "2026-10-19T10:00:00Z"
  },
  "request_id": "req_..."
}
```

The response must not echo credentials. Endpoint creation should
validate configuration and URL safety; it should not be required to call
the customer authentication endpoint synchronously. Provide an optional
test-connection API separately.

Errors: `400 VALIDATION_ERROR`, `401 UNAUTHENTICATED`, `403 FORBIDDEN`,
`413 CONFIG_TOO_LARGE`, `422 UNSAFE_DESTINATION`, `429 RATE_LIMITED`.

### 6.5 Publish event

`POST /v1/events` --- `202 Accepted` only after the `events` row and
`event.accepted` outbox row commit.

Request:

``` json
{
  "event_type": "payment.completed",
  "occurred_at": "2026-10-19T10:00:00Z",
  "schema_version": 1,
  "data": {
    "payment_id": "pay_123",
    "amount_minor_units": 1500,
    "currency": "INR"
  }
}
```

Headers include `Idempotency-Key: payment-pay_123-completed`.

Response:

``` json
{
  "data": {
    "event_id": "evt_...",
    "event_type": "payment.completed",
    "status": "ACCEPTED",
    "accepted_at": "2026-10-19T10:00:01Z"
  },
  "request_id": "req_..."
}
```

Errors: `400 INVALID_JSON`, `401 UNAUTHENTICATED`, `403 FORBIDDEN`,
`409 IDEMPOTENCY_KEY_REUSED`, `413 PAYLOAD_TOO_LARGE`,
`422 INVALID_EVENT_TYPE`, `429 RATE_LIMITED`,
`503 TEMPORARILY_UNAVAILABLE`.

The client does not choose an endpoint ID in the normal Model B publish
contract. RelayForge routes based on the authenticated organization plus
`event_type`.

### 6.6 Endpoint and subscription management

Required APIs: - `GET /v1/endpoints` -
`GET /v1/endpoints/{endpoint_id}` -
`PATCH /v1/endpoints/{endpoint_id}` -
`POST /v1/endpoints/{endpoint_id}/rotate-secret` (for RelayForge's
outbound webhook signing secret) - `GET /v1/subscriptions` - Optional
explicit subscription APIs if endpoint registration does not atomically
create subscriptions.

`POST /v1/endpoints` atomically creates an endpoint and
subscriptions for all provided `event_types`. If any event type is
invalid, roll back the entire registration rather than leaving a
partially registered endpoint. Subsequent subscription changes use
dedicated APIs.

Endpoint update semantics: - Use `If-Match` or `version` for optimistic
concurrency. - Omitted secret fields mean "leave unchanged"; explicit
secret rotation uses a dedicated operation. - Do not silently re-enable
a disabled endpoint during unrelated updates.

### 6.7 Delivery query and replay

`relayforge-delivery-api` owns delivery reads and replay writes. The
ingestion API composes its event acceptance response with a bounded
internal delivery summary read; it does not query the delivery database.

-   `GET /v1/events/{event_id}`: event status and delivery summary from
    ingestion acceptance plus delivery-owned progress.
-   `GET /v1/deliveries?status=&endpoint_id=&from=&to=&limit=&cursor=`:
    cursor-paginated delivery list.
-   `GET /v1/deliveries/{delivery_id}`: delivery state and attempts.
-   `POST /v1/deliveries/{delivery_id}/replay`: require an
    `Idempotency-Key` scoped to the source delivery, create a new
    generation, and return `202 Accepted`. The same key returns the same
    replay; a changed request body conflicts.

Replay request:

``` json
{"reason": "Destination outage resolved"}
```

Replay response:

``` json
{
  "data": {
    "original_delivery_id": "dlv_...",
    "replay_delivery_id": "dlv_...",
    "generation": 2,
    "status": "PENDING"
  },
  "request_id": "req_..."
}
```

Replay MUST check organization ownership, source delivery eligibility,
quotas, endpoint status, and replay idempotency. It must preserve the
original attempts and state.

### 6.8 Analytics API

-   `GET /v1/analytics/overview?from=&to=`
-   `GET /v1/analytics/deliveries/timeseries?from=&to=&interval=&endpoint_id=`

Analytics responses include `data_as_of` because ClickHouse is
eventually consistent. If ClickHouse is unavailable, return a bounded
`503 ANALYTICS_TEMPORARILY_UNAVAILABLE`; do not block the event or
delivery pipelines.

------------------------------------------------------------------------

## 7. Event and Kafka contracts

### 7.1 Kafka topics

Topic names and transport/recovery policy are fixed in
[`docs/decisions/kafka-transport.md`](docs/decisions/kafka-transport.md).
The topics are: - `relayforge.config.changed.v1` -
`relayforge.event.accepted.v1` -
`relayforge.delivery.ready.v1` -
`relayforge.analytics.delivery-facts.v1`

Use separate consumer groups for routing, delivery, and analytics. Use
`event_id` as the key for event routing and `delivery_id` as the key for
delivery work. Partition counts should be chosen based on measured
throughput and desired concurrency; changing keying or partition counts
can change ordering and is not an ordering guarantee.

### 7.2 Message envelope

``` json
{
  "message_id": "msg_...",
  "message_type": "event.accepted",
  "schema_version": 1,
  "occurred_at": "2026-10-19T10:00:01Z",
  "organization_id": "org_...",
  "aggregate_id": "evt_...",
  "correlation_id": "req_...",
  "payload": {
    "event_id": "evt_..."
  }
}
```

Prefer messages containing stable IDs and minimal metadata. Consumers
load authoritative state from PostgreSQL. Do not put plaintext
credentials or complete secret configuration in Kafka messages.

### 7.3 `event.accepted.v1`

Payload:

``` json
{
  "organization_id": "org_...",
  "event_id": "evt_...",
  "accepted_day": "2026-10-28",
  "accepted_at": "2026-10-28T10:00:01Z",
  "ingestion_locator_version": 1,
  "bucket_id": 3410,
  "map_epoch": 1,
  "ingestion_shard_id": "ing-a",
  "delivery_shard_id": "dlv-a",
  "event_type": "order.created",
  "payload_hash": "..."
}
```

Partition key is the opaque event locator, so messages for one event
remain together; cross-event ordering is not promised. The routing
consumer: 1. Validates the versioned event reference and organization
against ingestion. 2. Reads the payload through the bounded internal API
when filtering requires it. 3. Selects active subscriptions from the
delivery-owned projection and persists `routing_targets` and
`routing_progress` in one local transaction. 4. Creates deliveries and
`delivery.ready` outbox records in bounded local transactions using the
persisted targets. 5. Marks delivery-owned routing progress complete only
after every target is materialized. 6. Commits Kafka offset after durable
progress; recovery resumes unfinished progress. A missing event follows
the explicit retention/tombstone policy or bounded retry/error path.

### 7.4 `delivery.ready.v1`

Payload:

``` json
{"delivery_id":"dlv_..."}
```

The delivery consumer: 1. Loads/claims the delivery with a lease. 2. If
terminal, not due, or already owned by a live lease, treat the message
as a harmless duplicate and acknowledge it. 3. If eligible, executes one
delivery attempt. 4. Records outcome and analytics outbox facts
transactionally. 5. For retryable failure, sets `RETRY_SCHEDULED` and
`next_attempt_at`; it does not sleep. Retry scheduler will enqueue again
when due.

### 7.5 `analytics.delivery-facts.v1`

Payload contains fact ID, organization ID, event/delivery/endpoint IDs,
fact type, timestamp, attempt number, status, HTTP status, duration, and
bounded error code. It must not contain raw payloads, authentication
secrets, response bodies, or unnecessary PII.

Analytics consumers must tolerate duplicates. Use stable `fact_id`;
implement ClickHouse tables/queries with deduplication or idempotent
aggregation semantics. Do not assume a Kafka offset and ClickHouse
insert are one atomic transaction.

### 7.6 Outbox publisher algorithm

Use bounded batches and leases. A claim query can use
`FOR UPDATE SKIP LOCKED`:

``` sql
WITH batch AS (
  SELECT id
  FROM outbox
  WHERE status = 'PENDING'
    AND available_at <= now()
  ORDER BY created_at
  FOR UPDATE SKIP LOCKED
  LIMIT :batch_size
)
UPDATE outbox o
SET status = 'IN_FLIGHT',
    claimed_by = :worker_id,
    claim_token = gen_random_uuid(),
    claim_expires_at = now() + interval '30 seconds',
    publish_attempts = publish_attempts + 1
FROM batch
WHERE o.id = batch.id
RETURNING o.*;
```

Implementation must persist each claim token and use it in the final
update. Publish with Kafka producer acknowledgements appropriate for
durability (`acks=all`, idempotence enabled). After broker
acknowledgement, update the row to `PUBLISHED` conditional on the claim
token. If the publisher crashes after Kafka acknowledges but before
PostgreSQL is updated, it will publish a duplicate after lease recovery;
consumers must be idempotent.

Use a transaction per claim/update batch, not a long-running transaction
while waiting on Kafka.

------------------------------------------------------------------------

## 8. Transaction boundaries and idempotency

### 8.1 Organization creation

Single transaction: 1. Insert organization. 2. Generate API key and
store its hash/metadata. 3. Insert audit record. 4. Commit. 5. Return
plaintext key once.

If commit outcome is uncertain, do not blindly generate a second
organization/key. Provide a provisioning idempotency key or a safe
recovery path.

### 8.2 Endpoint registration

Single PostgreSQL transaction: 1. Validate request shape before
transaction. 2. Perform DNS/IP safety validation before transaction and
revalidate at delivery time. 3. Encrypt secrets or store them in Secret
Manager. 4. Insert endpoint. 5. Insert subscriptions for all event
types. 6. Insert audit record. 7. Commit and return `endpoint_id` and
event types.

If external Secret Manager creation occurs before the DB commit,
compensate orphaned secrets on failure or use a lifecycle cleanup job.
Do not hold DB locks while calling Secret Manager.

### 8.3 Event acceptance

Single ingestion-shard PostgreSQL transaction: 1. Authenticate request
before entering transaction. 2. Canonicalize request and calculate
`payload_hash`. 3. Claim `(organization_id, idempotency_key)` in the
shard-local 90-day registry. 4. If the key exists, compare its stored hash;
return the original event if equal, otherwise raise a conflict. 5. Insert
the event and `ingestion_outbox` row with `message_type=event.accepted`
and a versioned event reference. 6. Commit with the required synchronous
standby confirmation. 7. Return `202 Accepted`.

The event must never be acknowledged before the transaction is
committed.

### 8.4 Event routing

First delivery-shard transaction: claim the event routing operation,
resolve active subscriptions and filters from a ready projection, persist
the selected `routing_targets` and `routing_progress`, then commit. Process
the persisted targets in bounded chunks. Each chunk inserts deliveries
with generation `1` and conflict-safe uniqueness, writes matching
`delivery_outbox` rows, advances the durable cursor, and commits locally.
Mark routing complete only after the last target commits. A crash resumes
from the cursor; Kafka redelivery cannot recalculate a different target
set for the same event. Never update the ingestion-owned event row.

### 8.5 Delivery attempt lifecycle

Do not hold a DB transaction during outbound HTTP.

**Transaction A --- claim:** - Atomically update eligible delivery to
`IN_PROGRESS`. - Set `lease_owner`, random `lease_token`, and
`lease_expires_at`. - Increment `claim_count`; leave `attempt_count`
unchanged. - Commit.

**Outside transaction:** - Load endpoint configuration and secrets. -
Build URL/request. - Acquire token if needed. Authentication failures
produce authentication records but no webhook attempt.

**Transaction B --- reserve HTTP attempt:** - Verify the live lease token
and remaining `max_attempts` budget. - Increment `attempt_count` and
insert an `IN_PROGRESS` delivery attempt with that number. - Commit.
Then make the HTTP call outside a transaction with strict deadlines and
capture bounded response metadata. A controlled refresh-and-retry after
a destination 401 reserves another attempt. If a worker crashes after
reservation but before send, the attempt remains an unknown/aborted
attempt and consumes one attempt slot; this is preferable to an
unbounded retry loop and must be visible in history.

**Transaction C --- finalize:** - Lock delivery and verify current
`lease_token` matches. - Finalize the attempt record if HTTP was reserved;
for auth-only failure, persist an auth outcome without an HTTP attempt.
- If successful, mark
`SUCCEEDED` and set `completed_at`. - If retryable and within policy,
mark `RETRY_SCHEDULED` and set `next_attempt_at`. - If permanent or
retry limit reached, mark `DEAD_LETTERED`. - Insert analytics outbox
fact(s) in the same transaction. - Commit.

If lease token no longer matches, the stale worker must not overwrite
newer delivery state. Its result may be recorded as a late diagnostic
only if safely correlated; it must not change the logical state.

### 8.6 Duplicate and crash cases

  -----------------------------------------------------------------------
  Failure point                       Expected behavior
  ----------------------------------- -----------------------------------
  API process crashes before event    No accepted event; client may retry
  transaction commit                  

  API crashes after commit before     Client retries same idempotency key
  response                            and gets same event

  Outbox publisher crashes before     Row remains/reverts pending and is
  Kafka ack                           retried

  Publisher crashes after Kafka ack   Duplicate Kafka message possible;
  before marking published            consumer idempotency handles it

  Routing consumer crashes before DB  Kafka message redelivered; no
  commit                              partial transaction

  Routing consumer crashes after DB   Redelivery; unique delivery
  commit before offset commit         constraint prevents duplicate
                                      logical deliveries

  Delivery worker crashes before HTTP Lease expires; work is retried
  request                             

  Worker crashes after receiver       Duplicate webhook possible; stable
  processed request but before        `delivery_id` lets receiver
  success commit                      deduplicate

  Analytics consumer crashes after    Duplicate analytics fact possible;
  ClickHouse insert before offset     `fact_id` dedupe/aggregation policy
  commit                              handles it
  -----------------------------------------------------------------------

------------------------------------------------------------------------

## 9. Delivery state machine

### 9.1 States

-   `PENDING`: logical delivery created, not yet published for
    execution.
-   `ENQUEUED`: a delivery-ready message has been emitted or is in
    queue; DB remains authoritative.
-   `IN_PROGRESS`: worker owns a live lease and is executing.
-   `RETRY_SCHEDULED`: retryable failure recorded and `next_attempt_at`
    set.
-   `SUCCEEDED`: a configured success response was received.
-   `DEAD_LETTERED`: permanent failure or retry limit reached.
-   `CANCELLED`: endpoint disabled or operation cancelled according to
    explicit policy.

Allowed transitions:

``` text
PENDING -> ENQUEUED
ENQUEUED -> IN_PROGRESS
IN_PROGRESS -> SUCCEEDED
IN_PROGRESS -> RETRY_SCHEDULED
IN_PROGRESS -> DEAD_LETTERED
RETRY_SCHEDULED -> ENQUEUED
IN_PROGRESS --lease expiry--> RETRY_SCHEDULED or PENDING
PENDING/ENQUEUED/RETRY_SCHEDULED -> CANCELLED (only by explicit policy)
DEAD_LETTERED -> new replay generation (do not mutate original back to pending)
```

Do not require a state update to `ENQUEUED` to be perfectly synchronized
with Kafka; Kafka messages may be duplicated. Eligibility is determined
by PostgreSQL state and lease claims.

### 9.2 Claim query

`delivery_work` is the compact authoritative index of active due and
leased work; history remains in day-partitioned `deliveries`. This keeps
the hot claim/scheduler query from scanning up to 91 daily partitions.
Insert a work row with each new delivery and remove it in the same local
transaction as a terminal delivery result. All claim, retry, lease-renewal,
and terminal transitions lock `delivery_work` first, then the referenced
`deliveries` row by `(accepted_day, id)`, and update both or neither.
The work row is not a second source of delivery history; bounded periodic
reconciliation compares active deliveries with work rows by cohort and
alarms on any mismatch.
Example claim transaction:

``` sql
BEGIN;
SELECT accepted_day, delivery_id
FROM delivery_work
WHERE delivery_id = :delivery_id
  AND status IN ('PENDING', 'ENQUEUED', 'RETRY_SCHEDULED')
  AND next_attempt_at <= clock_timestamp()
  AND event_expires_at > clock_timestamp()
FOR UPDATE;
UPDATE delivery_work
SET status = 'IN_PROGRESS', lease_owner = :worker_id,
    lease_token = :lease_token,
    lease_expires_at = :lease_expires_at,
    claim_count = claim_count + 1, updated_at = clock_timestamp()
WHERE delivery_id = :delivery_id
  AND status IN ('PENDING', 'ENQUEUED', 'RETRY_SCHEDULED')
  AND next_attempt_at <= clock_timestamp()
  AND event_expires_at > clock_timestamp();
UPDATE deliveries
SET status = 'IN_PROGRESS', lease_owner = :worker_id,
    lease_token = :lease_token, lease_expires_at = :lease_expires_at,
    claim_count = claim_count + 1, updated_at = clock_timestamp(),
    version = version + 1
WHERE accepted_day = :accepted_day AND id = :delivery_id
  AND status IN ('PENDING', 'ENQUEUED', 'RETRY_SCHEDULED');
COMMIT;
```

The implementation binds one calculated lease expiry to both updates and
rolls back unless exactly one eligible work row and one matching delivery
row were updated. A production implementation must ensure a ready Kafka message cannot
bypass `next_attempt_at`; a non-due message should be acknowledged or
deferred without spinning, because the scheduler will emit a new message
when due.

### 9.3 Lease duration

Set a 240-second overall processing deadline per claim: token acquisition
at most 90 seconds, then at most two 60-second destination requests
(the second only after a 401), plus bounded database/finalization time.
Renew a 60-second lease with a compare-and-set on `lease_token` before
expiry while work is live. Stop sending if renewal fails or the overall
deadline is reached. A production configuration may lower these limits,
but must satisfy the same deadline/renewal invariant. Test slow token
exchange followed by slow webhook and a competing recovery worker.

Do not use unbounded token calls or HTTP calls. If a worker loses its
lease, it must not finalize state.

------------------------------------------------------------------------

## 10. Retry policy and failure classification

### 10.1 Separate authentication retries from delivery retries

Token acquisition has its own retry loop. A token failure does not count
as an HTTP attempt against the destination endpoint. However, it does
consume the delivery's overall deadline and must be observable.

Policy: token request gets an initial attempt plus up to **5 retries**
(6 attempts total) for transient failures, using exponential backoff with
jitter. The entire token acquisition phase has a hard **90-second total
deadline**, including network calls and backoff. Stop retrying when the
deadline is reached even if retry attempts remain. Permanent credential,
configuration, or malformed-response failures fail fast. - Webhook delivery:
configurable `max_attempts`, default 10, including the first attempt. -
No infinite retries. Retry horizon and maximum delivery age should also
be configurable at platform level.

### 10.2 Exponential backoff with full jitter

For retry number `n` starting at 0:

``` text
cap(n) = min(max_delay, base_delay * 2^n)
delay(n) = random(0, cap(n))
next_attempt_at = now + max(delay(n), bounded_retry_after)
```

Suggested starting values (validate through tests): -
`base_delay = 1 second` - `max_delay = 15 minutes` -
`max_retry_after = 1 hour` - `max_delivery_age = 24 hours`

Honor `Retry-After` for `429` and selected `503` responses, but clamp it
to the configured maximum. Use a stable injected clock and random source
in tests.

### 10.3 Classification

  -----------------------------------------------------------------------
  Outcome                             Default handling
  ----------------------------------- -----------------------------------
  2xx                                 `SUCCEEDED`

  Connection refused / DNS transient  Retry
  / connect timeout                   

  Read timeout / connection reset     Retry; duplicate effect is possible

  408                                 Retry

  429                                 Retry; honor bounded `Retry-After`

  500, 502, 503, 504                  Retry

  Other 5xx                           Retry by default, configurable

  Most 400/401/403/404/422            Terminal by default; allow
                                      endpoint-specific override only if
                                      safe

  Invalid URL / blocked destination / Terminal security/configuration
  TLS certificate invalid             failure; do not retry blindly

  Authentication token 401/403        Record auth failure; refresh
                                      credentials only according to
                                      strategy; avoid endless retries

  Token endpoint 429/5xx/network      Retry token acquisition within auth
  failure                             budget

  Token response missing access token Terminal auth configuration failure

  Payload rejected by destination     Usually terminal unless configured
                                      otherwise
  -----------------------------------------------------------------------

A `401` from the webhook endpoint may indicate an expired token. For
token-based auth, invalidate token and permit one controlled
refresh-and-retry of the same delivery attempt flow. Avoid repeated 401
loops.

### 10.4 Retry scheduler

The scheduler scans the compact work index with a bounded query:

``` sql
SELECT delivery_id, accepted_day
FROM delivery_work
WHERE status = 'RETRY_SCHEDULED'
  AND next_attempt_at <= now()
ORDER BY next_attempt_at
FOR UPDATE SKIP LOCKED
LIMIT :batch_size;
```

For each selected delivery, in the same transaction: 1. Transition
`delivery_work` and its day-partitioned `deliveries` row to `ENQUEUED`.
2. Insert a `delivery.ready` outbox record. 3. Commit.

Run a separate bounded scan of `delivery_work` for expired leases. Recover only rows where
`status='IN_PROGRESS' AND lease_expires_at < now()`. Apply a
compare-and-set to prevent racing a live worker heartbeat.
At `event_expires_at`, stop new sends, finalize active work with an
explicit terminal outcome after its lease resolves, publish final facts,
and remove its work row in the same local transaction.

------------------------------------------------------------------------

## 11. Authentication strategy design

### 11.1 Strategy interface

``` java
public interface AuthenticationStrategy {
    AuthResult apply(
        EndpointAuthConfig config,
        OutboundRequestContext context
    );
}

public sealed interface AuthResult {
    record Headers(Map<String, String> values) implements AuthResult {}
    record QueryParams(Map<String, String> values) implements AuthResult {}
}
```

In practice, prefer a method that returns a constrained immutable
`AuthMaterial` and let `OutboundRequestBuilder` apply it. Authentication
code should not independently issue the webhook request.

Implementations: - `NoAuthStrategy` - `ApiKeyStrategy` -
`BasicAuthStrategy` - `StaticBearerTokenStrategy` -
`CustomHeaderStrategy` - `OAuth2ClientCredentialsStrategy` -
`TokenExchangeStrategy` - Optional later: `HmacRequestSigningStrategy`,
`AwsSigV4Strategy`, `MtlsStrategy`

Resolve strategy using an enum/discriminator, never reflection based on
a customer-provided class name.

### 11.2 Generic token exchange

Configuration includes: - token request method (initially allow `POST`;
add other methods only when justified), - token URL, - content type, -
constrained headers, - body template with secret placeholders, -
access-token JSONPath, - optional expiry JSONPath, - optional
refresh-token path if explicitly supported later, - token placement
(header or query parameter; header is preferred), - prefix such as
`Bearer`, - timeout and bounded response size.

Do not support arbitrary JavaScript, shell commands, or executable
expressions. Restrict placeholders to approved secret references and a
small set of event-independent configuration values.

### 11.3 Token cache

Cache key should include:
`organization_id + endpoint_id + auth_config_version + credential_version`.

Store only the token value (with Redis access restricted and transport
protected), token type, expiry timestamp, and config/credential version.
Never log cache values. Set the cache TTL before token expiry and apply
bounded per-key jitter. For example, subtract a random fraction of a
configured jitter window from the safe expiry time; never let the cache
TTL extend beyond the token's actual expiry.

Stampede controls are layered:

1. **Local single-flight:** concurrent requests in one worker for the
   same cache key share one in-flight token acquisition.
2. **Distributed coordination:** use a short-lived Redis lock keyed by
   the full token cache key when coordinating across workers. The lock
   has a unique owner token and bounded wait; release it only if the
   owner token still matches.
3. **Double-check after lock:** after acquiring the lock, re-read Redis
   because another worker may already have refreshed the token.
4. **Early refresh:** where safe, refresh before hard expiry with
   randomized scheduling; callers may continue using a still-valid token
   during refresh.
5. **Bounded fallback:** if Redis or lock coordination is unavailable,
   use local single-flight and bounded retries/backoff rather than
   launching unbounded duplicate token requests.

Use per-token-configuration keys, not one lock per organization, so
unrelated endpoints do not block one another. TTL jitter spreads expiries
across endpoints/organizations; single-flight and distributed locking
prevent a single expired key from triggering a burst of duplicate token
requests. Cache invalidation is achieved through versioned keys on auth
configuration/secret changes; old entries expire naturally and may be
explicitly evicted as an optimization.

For tokens with no reliable expiry: - use a configured TTL, or - do not
cache. Do not assume every opaque token is a JWT. Do not trust an
unverified JWT claim as the sole source of authorization or expiry.

### 11.4 Secret lifecycle

-   Encrypt endpoint credentials and signing secrets with Java 21
    `AES/GCM/NoPadding`, a fresh random 256-bit data key per configuration
    version, a fresh 96-bit nonce per encryption, and a 128-bit tag. Bind
    organization ID, endpoint ID, field name, and configuration version
    as authenticated associated data. Persist ciphertext, nonce, wrapped
    data key, and key version; never persist plaintext keys.
-   A production `KeyEncryptionService` adapter wraps/unwraps data keys
    through an external managed key service. Its provider is deployment
    configuration, so no cloud vendor is baked into the data format.
    The local provider reads a test-only key from the development
    environment and must refuse production mode. Key-service failure
    prevents new secret writes and makes unavailable secrets retryable
    delivery work rather than sending without authentication.
-   Rotate the wrapping key by rewrapping stored data keys. Rotate a
    compromised endpoint secret by replacing its encrypted value,
    incrementing configuration/credential versions, invalidating token
    caches, and recording a redacted audit entry. Keep old wrapping keys
    available until all rows and retained backups that depend on them
    have been rewrapped or expired.
-   Include a config/credential version in cache keys so secret rotation
    invalidates old tokens.
-   Never return credentials from `GET /v1/endpoints`.
-   For update requests, omitted secrets mean unchanged; use explicit
    secret replacement.
-   Redact auth request bodies and responses from application logs.
-   Token request destinations require the same SSRF protections as
    webhook destinations.

Use `com.jayway.jsonpath:json-path:3.0.0` only to read bounded token
responses. Accept definite property/index paths; reject filters,
recursive descent, functions, scripts, and paths that return multiple
values. Validate paths at endpoint registration and cap response bytes
before parsing. [Java GCM parameters](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/javax/crypto/spec/GCMParameterSpec.html),
[JsonPath 3.0.0 artifact](https://central.sonatype.com/artifact/com.jayway.jsonpath/json-path/3.0.0).

------------------------------------------------------------------------

## 12. Outbound request construction and SSRF defenses

### 12.1 Request composition order

1.  Load active endpoint and its current configuration version.
2.  Confirm endpoint belongs to delivery's organization.
3.  Resolve and validate destination host and IP addresses.
4.  Build URL from `base_url + path`, then apply configured query
    parameters using a URI builder.
5.  Apply safe default headers.
6.  Apply configured non-secret headers, excluding reserved/forbidden
    headers.
7.  Resolve authentication and attach token/API key/signature material.
8.  Serialize webhook envelope deterministically.
9.  Compute HMAC signature over the exact raw bytes sent.
10. Send with connection and response deadlines; capture bounded
    metadata.
11. Persist sanitized outcome.

### 12.2 SSRF requirements

-   Require HTTPS for production destinations by default.
-   Reject loopback, private, link-local, multicast, unspecified,
    IPv4-mapped IPv6, and cloud metadata addresses unless a deliberately
    controlled development mode permits them.
-   Resolve and validate all A/AAAA records and connect only to a
    validated address while preserving correct TLS hostname
    verification.
-   Revalidate on each delivery or use a very short safe DNS cache; do
    not rely solely on registration-time DNS validation.
-   Disable redirects by default. If redirects are later supported,
    validate each redirect target from scratch and strip credentials
    when host changes.
-   Block user-controlled `Host`, `Connection`, `Content-Length`, proxy
    headers, and hop-by-hop headers.
-   Apply egress network policy as defense in depth.

### 12.3 Webhook signature

Suggested headers: - `X-RelayForge-Event-Id` -
`X-RelayForge-Delivery-Id` - `X-RelayForge-Event-Type` -
`X-RelayForge-Timestamp` -
`X-RelayForge-Signature: v1=<hex-hmac-sha256>`

Canonical input:

``` text
<unix_timestamp>.<exact_raw_request_body_bytes>
```

Receiver verification should use constant-time comparison and a
timestamp tolerance. Signature secret is distinct from the customer's
destination authentication credentials. Store it encrypted and show it
once only.

------------------------------------------------------------------------

## 13. Java implementation details

### 13.1 Transaction annotations

Use `@Transactional` on application service methods that perform
database-only state transitions. Avoid self-invocation traps with Spring
proxy transactions. Keep HTTP calls, Kafka publish calls, and ClickHouse
inserts outside PostgreSQL transactions.

Example event acceptance:

``` java
@Service
@RequiredArgsConstructor
public class EventIngestionService {
    private final EventRepository events;
    private final OutboxRepository outbox;
    private final IdempotencyHasher hasher;

    @Transactional
    public AcceptedEvent accept(
            UUID organizationId,
            PublishEventCommand command) {
        byte[] hash = hasher.hashCanonical(command);
        var existing = events.findByOrganizationAndIdempotencyKey(
                organizationId, command.idempotencyKey());

        if (existing.isPresent()) {
            if (!MessageDigest.isEqual(existing.get().payloadHash(), hash)) {
                throw new IdempotencyKeyReusedException();
            }
            return AcceptedEvent.from(existing.get());
        }

        Event event = Event.accepted(organizationId, command, hash);
        events.insert(event);
        outbox.insert(OutboxMessage.eventAccepted(event));
        return AcceptedEvent.from(event);
    }
}
```

The unique constraint is still required because a read-before-insert
check alone is race-prone. Catch the unique-constraint conflict and
reload the existing row in a new transaction or use an
`INSERT ... ON CONFLICT` pattern. Do not treat every database integrity
error as an idempotency conflict.

### 13.2 Repository guidelines

-   Every organization-owned repository method takes `organizationId`
    explicitly.
-   Prefer methods such as `findByIdAndOrganizationId`, not `findById`
    for API-facing queries.
-   Use optimistic locking for endpoint/subscription updates.
-   Use explicit SQL for outbox claim, delivery claim, retry scheduling,
    and expired-lease recovery.
-   Avoid N+1 queries when routing fan-out; fetch endpoint/subscription
    IDs in bounded batches.
-   Set JDBC statement/query timeouts.
-   Keep database transactions short.

### 13.3 HTTP client

Use a bounded, reusable HTTP client (Spring `WebClient` or Java
`HttpClient`) with: - connection pool limits, - connect timeout, -
response/read timeout, - overall attempt deadline, - maximum
response-body bytes, - TLS certificate validation, - redirects disabled
by default, - no ambient proxy credentials, - metrics by destination
class without exposing full URLs with secrets.

Never create a new HTTP client per delivery. Never allow customer
configuration to set unbounded timeout values.

### 13.4 Kafka consumer behavior

-   Disable auto-commit; commit offsets only after the consumer's
    database transaction succeeds.
-   Make consumer handlers idempotent.
-   Configure bounded poll records and concurrency.
-   The poll thread must continue polling while slow HTTP attempts run
    in a bounded worker pool. Pause assigned partitions when that pool
    is full; resume when capacity returns. Commit each partition's
    offsets only through its contiguous completed records, never past
    an unfinished attempt. Handle rebalances without losing lease
    ownership; duplicate work remains safe after recovery. Set and test
    `max.poll.interval.ms` against the poll loop, not the HTTP deadline.
-   Retry transient infrastructure errors with bounded delay; route
    poison messages to a dead-letter topic or durable failure record
    after a bounded policy.
-   Do not endlessly retry a malformed message on the same partition,
    blocking all later messages.
-   Do not rely on Kafka exactly-once semantics to make PostgreSQL
    writes atomic.

------------------------------------------------------------------------

## 14. Analytics and ClickHouse

### 14.1 Analytics facts

Emit append-oriented facts for: - `event.accepted` -
`delivery.created` - `delivery.attempt.finished` -
`delivery.succeeded` - `delivery.retry_scheduled` -
`delivery.dead_lettered` - `delivery.replayed`

Use stable `fact_id`, organization/event/delivery/endpoint IDs, fact
timestamp, status, attempt number, HTTP status, duration, and bounded
error code. Do not send secrets or full event payloads.

### 14.2 Database and table schema

Use a dedicated ClickHouse database so analytical storage and permissions
are separated from the transactional PostgreSQL database. The initial
schema below is a single-node/local-table baseline; if deployment later
uses a ClickHouse cluster, define the replicated/distributed topology in
deployment configuration rather than assuming it here.

```sql
CREATE DATABASE IF NOT EXISTS relayforge_analytics;

CREATE TABLE IF NOT EXISTS relayforge_analytics.delivery_facts
(
    organization_id UUID,
    event_date      Date,
    fact_id         UUID,
    version         UInt64,

    event_id        Nullable(UUID),
    delivery_id     Nullable(UUID),
    endpoint_id     Nullable(UUID),
    replay_generation Nullable(UInt32),

    fact_type       LowCardinality(String),
    status          LowCardinality(String),
    attempt_number  Nullable(UInt16),
    http_status     Nullable(UInt16),
    duration_ms     Nullable(UInt32),
    error_code      LowCardinality(String) DEFAULT '',

    occurred_at     DateTime64(3, 'UTC'),
    ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(event_date)
ORDER BY (organization_id, event_date, fact_id);
```

**Column contract:**

- `fact_id` is generated by RelayForge for one logical analytics fact
  and reused unchanged when Kafka redelivers that fact.
- `version` is stable for identical redeliveries. Only a correction or
  update to the same logical fact increments it. Do not use ingestion
  time as the version because a duplicate could incorrectly supersede a
  newer fact.
- `event_date` is derived from `occurred_at` in UTC and is immutable for
  a given `fact_id`. This ensures duplicate rows land in the same
  partition, which is necessary for replacement semantics.
- `fact_type` values include `event.accepted`, `delivery.created`,
  `delivery.attempt.finished`, `delivery.succeeded`,
  `delivery.retry_scheduled`, `delivery.dead_lettered`, and
  `delivery.replayed`.
- `error_code` is a bounded, sanitized classification, never raw error
  text. Do not store API keys, tokens, authentication bodies, or full
  event payloads in ClickHouse.

### 14.3 Ingestion and deduplication contract

The analytics consumer validates each fact, inserts it into
`relayforge_analytics.delivery_facts`, and commits the Kafka offset only
after ClickHouse confirms the insert. If the insert succeeds but offset
commit fails, the same `fact_id` and `version` are inserted again; this
is expected and must be safe.

`ReplacingMergeTree(version)` removes duplicate sorting keys during
background merges, not synchronously at insert time. Use `FINAL` for
correctness-sensitive queries and benchmark its cost. Do not assume a
normal `SELECT` immediately returns one row per `fact_id`. Corrections
must increment `version`; producers must generate identical content for
the same `(fact_id, version)`. Validate that invariant in contract tests;
ClickHouse does not enforce uniqueness at insert time.

Example query returning deduplicated facts for an organization and time
range; `FINAL` selects all nullable fields from the same row:

```sql
SELECT
    fact_id, fact_type, status, event_id, delivery_id,
    endpoint_id, occurred_at, http_status, duration_ms
FROM relayforge_analytics.delivery_facts FINAL
WHERE organization_id = {organization_id:UUID}
  AND event_date >= {start_date:Date}
  AND event_date <= {end_date:Date};
```

The API layer must always inject the authenticated organization ID; it
must not trust an organization ID supplied by the caller. Bound time
ranges and query timeouts are required. A future aggregate optimization
must select an entire latest row with deterministic tie handling, not
call `argMax` separately on nullable columns.

### 14.4 Optional daily rollup table (deferred)

Do not create an incremental materialized view directly over the raw
fact table for correctness-sensitive counts in the initial release: duplicate Kafka
inserts can be counted before `ReplacingMergeTree` background merges
remove them. If dashboard load later justifies pre-aggregation, add a
`delivery_daily_rollups` table populated by a repeatable scheduled job
that first deduplicates source facts by `fact_id` and then upserts or
rebuilds the affected time buckets idempotently. The rollup schema and
refresh strategy are deferred until measured query patterns justify
this additional state.

### 14.5 Analytics isolation

- Analytics consumer lag must not slow event routing or delivery.
- ClickHouse query APIs require organization predicates, bounded time
  ranges, interval limits, and query timeouts.
- Include `data_as_of` in responses to expose analytics freshness.
- Individual event and delivery status endpoints read PostgreSQL, not
  ClickHouse.
- Integration tests must prove duplicate ingestion does not inflate
  metrics exposed by RelayForge.

------------------------------------------------------------------------

## 15. Security and authorization

### 15.1 Authentication and authorization

-   Resolve organization from the presented API key.
-   Check key revocation, expiry, scopes, and organization status.
-   Never accept organization identity from request body as
    authoritative.
-   Apply ownership predicates to every endpoint, event, subscription,
    delivery, and replay operation.
-   Use indistinguishable `404` responses for resources that do not
    exist or are not owned by the caller where revealing existence could
    leak information.
-   Protect organization creation with a separate provisioning/signup
    control.
-   Separate operator privileges from organization API keys.

### 15.2 Payload and request limits

Initial limits: - Event
payload: 256 KiB. - Endpoint/auth configuration: 64 KiB. - Token
response: 64 KiB. - Customer response excerpt stored: 4 KiB. - API list
page size: default 50, maximum 200. - Time-series query: bounded time
range and maximum point count. - Endpoint request timeout: 100--10,000
ms by default, hard maximum 60,000 ms. - Max endpoint concurrency:
capped per plan and global policy.

### 15.3 Logging

Log request ID, organization ID, endpoint ID, event ID, delivery ID,
attempt number, status code, duration, and safe error code. Do not
log: - API keys or signing secrets, - access tokens, - authentication
request/response bodies, - full payload by default, - query strings that
may contain API keys, - unrestricted customer response bodies.

------------------------------------------------------------------------

## 16. Observability and operational behavior

### 16.1 Quota lookup and cache behavior

PostgreSQL `organizations.quota_config` is authoritative. Redis caches
resolved quota configuration by organization ID and configuration
version with a bounded TTL. Organization creation resolves plan defaults
and applies any validated explicit overrides. On a cache miss, load from
PostgreSQL and populate Redis. Redis also coordinates the distributed
per-organization event token bucket and in-flight delivery permits.
PostgreSQL can supply the configuration during a cache outage, but it
cannot by itself enforce a distributed rate counter. If Redis
coordination is unavailable, reject new quota-sensitive event writes
with a retryable error and pause new worker claims; already claimed
deliveries may finish. Reconcile permits from PostgreSQL lease state on
recovery. If PostgreSQL is unavailable, fail closed for quota-sensitive
write operations rather than bypassing limits.
Invalidate or version-bump the cache when quota configuration changes.
The initial plan values below are configuration defaults, subject to
functional tests and the final load benchmark. They bound each tenant;
the global admission ceiling is set from measured capacity.

| Plan/limit | Events per minute | Burst events | Active endpoints | In-flight deliveries |
|---|---:|---:|---:|---:|
| `FREE` default | 600 | 20 | 25 | 100 |
| `STANDARD` default | 6,000 | 200 | 100 | 1,000 |
| Platform maximum for an approved override | 60,000 | 2,000 | 1,000 | 20,000 |

Use a per-organization token bucket with a refill rate of
`events_per_minute / 60` and capacity `burst_events`; do not treat the
one-minute allowance as a one-request burst. Enforce endpoint count in
the control-plane transaction. In-flight permits gate worker claims,
while PostgreSQL delivery leases remain the source of truth for recovery.
At
the planning profile, 100 STANDARD accounts at 100 events/s each reach
the 10,000 events/s peak; the 15,000 events/s headroom test uses explicit
overrides. These arithmetic allocations do not prove backend capacity.

### 16.2 Metrics

API: - request rate, p50/p95/p99 latency, status code, validation
failures, idempotency conflicts, rate-limit rejects.

PostgreSQL/outbox: - pool utilization, wait time, transaction duration,
lock waits, slow queries, pending outbox count/oldest age, publish
attempts, expired outbox claims.

Kafka: - consumer lag, rebalances, publish errors, partition skew,
dead-letter count.

Delivery: - attempts/s, successful deliveries/s, retry rate, terminal
failure rate, oldest pending delivery, expired leases, endpoint latency,
timeout rate, token acquisition latency/failure, 401-refresh rate.

Analytics: - analytics consumer lag, ClickHouse insert latency/errors,
query latency, `data_as_of` age.

### 16.3 Correlation fields

Propagate `request_id`, `event_id`, `delivery_id`, `endpoint_id`,
`organization_id`, `attempt_number`, and `correlation_id` across logs
and message envelopes. Do not use raw customer payload values as metric
labels.

### 16.4 Health endpoints

-   `/health/live`: process is running.
-   `/health/ready`: critical dependencies (especially PostgreSQL) allow
    safe serving. Kafka readiness behavior should reflect the operation
    being served. ClickHouse outage should degrade analytics, not
    necessarily make ingestion unready.
-   Health responses must not disclose credentials, internal IPs, query
    text, or stack traces.

------------------------------------------------------------------------

## 17. Testing strategy

### 17.1 Unit tests

-   API key generation/hash/verification and scope evaluation.
-   Endpoint URL normalization and SSRF address classification.
-   Authentication strategy selection and token response extraction.
-   Token expiry and refresh behavior.
-   Idempotency canonicalization.
-   Retry classification, exponential backoff, jitter bounds, and
    `Retry-After` clamping.
-   Delivery state-machine transitions and stale lease rejection.
-   Webhook signature byte-level canonicalization.
-   Subscription filter semantics.

Use injected `Clock`, `RandomGenerator`, HTTP transport, and token cache
abstractions for deterministic tests.

### 17.2 Integration tests

Use Testcontainers for PostgreSQL, Kafka, Redis, and (where practical)
ClickHouse.

Must cover: - Event + outbox atomicity. - Same idempotency key and same
body returns same event. - Same key with different body returns
conflict. - Routing creates one delivery per matching active endpoint. -
Duplicate Kafka event message does not create duplicate delivery. -
Delivery result and analytics outbox fact commit atomically. - Outbox
publisher duplicate publish is safe. - Retry scheduler creates one ready
message per eligible transition. - Expired lease recovery and stale
worker cannot overwrite state. - Secret redaction in logs and API
responses. - Cross-organization access is rejected. - SSRF validation
blocks loopback/private/metadata destinations. - Replay creates a new
generation and preserves original attempts. - ClickHouse duplicate fact
ingestion does not inflate metrics.

### 17.3 Contract tests

-   Validate JSON request/response contracts for all public APIs.
-   Validate Kafka message schemas and schema version compatibility.
-   Ensure secret fields are never present in endpoint read responses.
-   Verify API error codes and status codes remain stable.

### 17.4 Load and failure tests

Measure rather than claim: - event ingestion rate and p95/p99 latency, -
outbox publishing throughput and oldest backlog age, - routing fan-out
throughput, - delivery attempts/s under receiver latency/failure
mixes, - token cache hit ratio and token endpoint amplification, -
PostgreSQL pool/lock behavior, - analytics lag and ClickHouse insert
throughput.

Failure injection: - kill publisher after Kafka ack but before marking
outbox published, - kill router after DB commit before offset commit, -
kill worker after receiver returns but before result commit, -
expire/renew leases under concurrency, - throttle or disconnect Redis, -
make ClickHouse unavailable, - make token endpoint return 429/500 or
malformed token responses, - return webhook 401/429/500/timeouts.

------------------------------------------------------------------------

## 18. Implementation sequence

### Milestone 1 --- API and durable acceptance

-   Organization/API-key provisioning.
-   Endpoint registration and event-type subscriptions.
-   Event publish endpoint with idempotency.
-   PostgreSQL schema and event outbox.
-   Basic outbox publisher and event routing.

### Milestone 2 --- Basic delivery

-   Delivery table and ready topic.
-   No-auth and static API key/Bearer/Basic strategies.
-   Outbound request construction and webhook signing.
-   Attempt history and delivery query APIs.

### Milestone 3 --- Reliability

-   Retry policy, due-work scanner, leases, expired-lease recovery.
-   Dead-lettering and replay generation.
-   Duplicate-message and crash-recovery integration tests.

### Milestone 4 --- Dynamic authentication

-   OAuth client credentials and generic token exchange.
-   Encrypted credential storage / Secret Manager integration.
-   Token cache, single-flight refresh, auth-attempt diagnostics.
-   SSRF and secret-redaction security tests.

### Milestone 5 --- Analytics

-   Analytics outbox facts.
-   Kafka analytics consumer and ClickHouse schema.
-   Overview and time-series endpoints with freshness metadata.

### Milestone 6 --- Production readiness

-   Metrics, structured logs, dashboards, alerts.
-   Docker Compose and CI.
-   Load tests, receiver simulator, failure-injection suite.
-   Benchmark report with measured results and bottleneck analysis.

------------------------------------------------------------------------

## 19. Decisions incorporated and remaining implementation notes

The following final decisions are part of this LLD:

1. User provisioning uses a trusted `POST /v1/users` endpoint with an
   `organization_id` and user metadata. Organization creation remains a
   protected provisioning operation; no anonymous organization signup.
2. Endpoint registration atomically creates the endpoint and all
   subscriptions for its supplied `event_types`; any failure rolls back
   the whole transaction.
3. Authentication configuration secrets use application-level envelope
   encryption.
4. Token exchange uses declarative allowlisted fields and secret
   references; no arbitrary templates, scripts, or event-payload
   interpolation. JSONPath is only for token-response extraction.
5. Token acquisition allows an initial attempt plus up to five retries
   for transient failures, using exponential backoff with jitter within
   a 90-second total authentication deadline. Permanent configuration
   errors fail fast.
6. Any HTTP 2xx response is successful by default.
7. HTTP 408/429 and transient network/5xx failures are retryable; other
   4xx responses are terminal by default, subject to the existing
   documented classifications.
8. Routing snapshots matching subscriptions and endpoint identifiers;
   the worker still checks current endpoint status/config before sending.
9. Implement and test a 90-day acceptance-based lifecycle for published
   events and delivery history. Configuration tables are not age-purged.
10. Purge eligible published outbox rows on an operational schedule;
    unpublished rows are never purged by age alone.
11. No ordering guarantee in the initial release.
12. Quotas are persisted in PostgreSQL (`organizations.quota_config`)
    and cached in Redis. Plan defaults apply unless overrides are
    explicitly supplied during organization creation. Redis is an
    optimization; behavior on cache failure must remain bounded and
    enforce safe platform defaults.
13. Analytics facts use `ReplacingMergeTree(version)` and stable
    `fact_id`; query-time deduplication is required where eventual merge
    semantics are insufficient.
14. Replay creates a new delivery ID and incremented generation while
    preserving the original delivery and attempt history.
15. Token cache keys include organization, endpoint, auth configuration
    version, and credential version. TTL jitter, local single-flight,
    per-key distributed coordination, cache re-check after lock, and
    early refresh reduce stampedes.

The remaining work is implementation and measurement: convert
[`docs/schema/`](docs/schema/README.md) to service-owned Flyway migrations,
bind the production key provider to the deployment environment, and test
the documented quota and security contracts. Measure ClickHouse query-time
deduplication, retention cleanup, disk growth, and `FINAL` cost after the
functional and end-to-end suites pass. Those outcomes may require a
reviewed design change; no throughput or recovery time is claimed here.

------------------------------------------------------------------------

## 20. Definition of done

The first end-to-end release is complete when:

-   An organization can be provisioned and use its API key.
-   It can register an endpoint and one or more event types with
    supported authentication configuration.
-   Publishing an event durably stores the event and outbox before
    returning `202`.
-   RelayForge routes by organization and event type to all active
    matching subscriptions.
-   Duplicate event submissions and Kafka messages do not create
    duplicate logical deliveries.
-   A delivery can be sent, signed, retried, dead-lettered, queried, and
    replayed.
-   Token exchange works with safe caching, expiry, bounded retries, and
    secret redaction.
-   Worker crashes and publisher duplicates are handled according to the
    documented at-least-once model.
-   Organization isolation, SSRF, payload limits, and credential
    protections pass tests.
-   Analytics facts reach ClickHouse asynchronously and responses expose
    freshness.
-   Load tests produce reproducible measured results; no unmeasured
    performance targets are claimed.

------------------------------------------------------------------------

## References

The design is based on RelayForge High-Level Design and the
implementation checklist. External technology documentation to consult
during implementation:

-   PostgreSQL: https://www.postgresql.org/docs/
-   Apache Kafka: https://kafka.apache.org/documentation/
-   Spring Boot: https://docs.spring.io/spring-boot/
-   ClickHouse: https://clickhouse.com/docs
-   Redis: https://redis.io/docs/
-   Google Cloud Secret Manager:
    https://cloud.google.com/secret-manager/docs

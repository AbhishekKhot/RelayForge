# RelayForge --- Low-Level Design (LLD)

**Document version:** 1.3\
**Status:** Implementation baseline — aligned with final v1 decisions\
**Aligned documents:** RelayForge Technical Specification v3.0;
RelayForge High-Level Design v3.0\
**Primary stack:** Java 21, Spring Boot, PostgreSQL, Kafka, Redis,
ClickHouse\
**Design principle:** PostgreSQL is authoritative; Kafka transports
work; ClickHouse serves eventually consistent analytics.

------------------------------------------------------------------------

## 1. Purpose and scope

This document translates the RelayForge HLD into implementation-level
design. It defines module boundaries, Java packages, database
structures, API contracts, message envelopes, transaction boundaries,
delivery state transitions, authentication handling, retry behavior,
security controls, and testing requirements.

This is the implementation contract for RelayForge v1. Implementation and benchmark completion are tracked separately in `RelayForge_Implementation_Checklist.md`.

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

### 1.2 Out of scope for v1

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
  Decision                v1 choice                        Reason
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

Use a modular monolith codebase with clear boundaries. Worker processes
can launch different Spring profiles or entry points while reusing the
same domain/application modules.

``` text
relayforge/
├── relayforge-api/
│   └── src/main/java/com/relayforge/api/
│       ├── RelayForgeApiApplication.java
│       ├── config/
│       ├── security/
│       ├── controller/
│       │   ├── OrganizationController.java
│       │   ├── EndpointController.java
│       │   ├── EventController.java
│       │   ├── DeliveryController.java
│       │   └── AnalyticsController.java
│       ├── dto/
│       └── exception/
├── relayforge-core/
│   └── src/main/java/com/relayforge/
│       ├── organization/
│       │   ├── domain/
│       │   ├── application/
│       │   ├── persistence/
│       │   └── security/
│       ├── endpoint/
│       ├── subscription/
│       ├── event/
│       ├── delivery/
│       ├── authentication/
│       ├── outbox/
│       ├── replay/
│       ├── analytics/
│       ├── security/
│       └── common/
├── relayforge-outbox-publisher/
├── relayforge-routing-worker/
├── relayforge-delivery-worker/
├── relayforge-retry-scheduler/
├── relayforge-analytics-consumer/
├── relayforge-contracts/
│   └── src/main/java/com/relayforge/contracts/
│       ├── events/
│       ├── delivery/
│       └── analytics/
├── db/migration/
├── deploy/
├── load-tests/
└── docs/
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

The following schema is a logical baseline; column sizes and
partitioning should be adjusted after benchmarks.

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
identity lifecycle; those remain outside the delivery pipeline's v1
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
    version             BIGINT NOT NULL DEFAULT 1
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
is not used in the v1 envelope-encryption path. - Do not allow untrusted
users to override reserved RelayForge headers (`X-RelayForge-*`, `Host`,
`Content-Length`, `Connection`, etc.). - Endpoint URL validation must be
repeated at delivery time to defend against DNS changes; do not rely
only on registration-time checks.

### 5.5 `subscriptions`

``` sql
CREATE TABLE subscriptions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id),
    endpoint_id     UUID NOT NULL REFERENCES endpoints(id),
    event_type      VARCHAR(200) NOT NULL,
    filter_config   JSONB,
    status          VARCHAR(24) NOT NULL DEFAULT 'ACTIVE'
                    CHECK (status IN ('ACTIVE', 'PAUSED', 'DELETED')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    version         BIGINT NOT NULL DEFAULT 1,
    UNIQUE (organization_id, endpoint_id, event_type)
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
    organization_id UUID NOT NULL REFERENCES organizations(id),
    event_type      VARCHAR(200) NOT NULL,
    occurred_at     TIMESTAMPTZ,
    schema_version  INTEGER NOT NULL DEFAULT 1,
    payload         JSONB NOT NULL,
    payload_hash    BYTEA NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    status          VARCHAR(24) NOT NULL DEFAULT 'ACCEPTED'
                    CHECK (status IN ('ACCEPTED', 'ROUTING', 'ROUTED', 'NO_SUBSCRIBERS')),
    accepted_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (organization_id, idempotency_key)
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

For v1, payloads are stored in PostgreSQL JSONB with a strict size cap.
The documented default retention target for event payloads and
associated delivery diagnostics is 30 days. **Automated data purge is
deferred in v1**; this is a policy target, not an implemented cleanup
job. Consider object storage only if measured payload size/retention
requires it.

### 5.7 `deliveries`

``` sql
CREATE TABLE deliveries (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id     UUID NOT NULL REFERENCES organizations(id),
    event_id            UUID NOT NULL REFERENCES events(id),
    endpoint_id         UUID NOT NULL REFERENCES endpoints(id),
    subscription_id     UUID REFERENCES subscriptions(id),
    generation          INTEGER NOT NULL DEFAULT 1,
    status              VARCHAR(32) NOT NULL DEFAULT 'PENDING'
                        CHECK (status IN (
                            'PENDING', 'ENQUEUED', 'IN_PROGRESS',
                            'RETRY_SCHEDULED', 'SUCCEEDED', 'DEAD_LETTERED',
                            'CANCELLED'
                        )),
    attempt_count       INTEGER NOT NULL DEFAULT 0,
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

The unique constraint ensures one logical delivery per
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
    endpoint_id         UUID NOT NULL REFERENCES endpoints(id),
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

Document a default 30-day retention target for outbox rows, then purge
eligible records after that period. **Automated purge is deferred in v1**;
records are not deleted by this implementation. Never delete a pending
or unpublished row merely because it is old.

### 5.11 `replay_requests` and `audit_records`

``` sql
CREATE TABLE replay_requests (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id     UUID NOT NULL REFERENCES organizations(id),
    original_delivery_id UUID NOT NULL REFERENCES deliveries(id),
    replay_delivery_id  UUID NOT NULL REFERENCES deliveries(id),
    requested_by_key_id UUID REFERENCES api_keys(id),
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
-   Add a composite ownership constraint where it materially protects
    against mismatched organization/endpoint references.
-   Use Flyway migrations with forward-only schema changes in normal
    deployment.
-   Add indexes based on query plans and load tests; each index adds
    write and storage overhead.
-   Use `TIMESTAMPTZ` and UTC.
-   Use optimistic version columns for editable endpoint/subscription
    configuration.
-   Avoid using JSONB for high-cardinality fields used frequently in
    joins or predicates.
-   Do not partition tables preemptively. Measure row counts, vacuum
    behavior, and query latency first.

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

Recommended v1: `POST /v1/endpoints` atomically creates endpoint and
subscriptions for all provided `event_types`. If any event type is
invalid, roll back the entire registration rather than leaving a
partially registered endpoint. Subsequent subscription changes use
dedicated APIs.

Endpoint update semantics: - Use `If-Match` or `version` for optimistic
concurrency. - Omitted secret fields mean "leave unchanged"; explicit
secret rotation uses a dedicated operation. - Do not silently re-enable
a disabled endpoint during unrelated updates.

### 6.7 Delivery query and replay

-   `GET /v1/events/{event_id}`: event status and delivery summary from
    PostgreSQL.
-   `GET /v1/deliveries?status=&endpoint_id=&from=&to=&limit=&cursor=`:
    cursor-paginated delivery list.
-   `GET /v1/deliveries/{delivery_id}`: delivery state and attempts.
-   `POST /v1/deliveries/{delivery_id}/replay`: create a new generation,
    return `202 Accepted`.

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

Suggested topic names: - `relayforge.event.accepted.v1` -
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
{"event_id":"evt_..."}
```

Partition key: `event_id` or organization-aware key if partition
distribution is needed. The routing consumer: 1. Loads event by ID. 2.
Returns success for a missing/deleted event only if the deletion policy
explicitly allows tombstones; otherwise send to bounded retry/DLQ
handling. 3. Finds subscriptions where
`organization_id = event.organization_id`,
`event_type = event.event_type`, and status is `ACTIVE`. 4. Applies the
supported deterministic filter, if configured. 5. In one transaction,
creates one delivery per matching endpoint and its `delivery.ready`
outbox record. 6. Updates event routing status to `ROUTED` or
`NO_SUBSCRIBERS`. 7. Commits DB transaction, then commits Kafka offset.

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

Single PostgreSQL transaction: 1. Authenticate request before entering
transaction. 2. Canonicalize request and calculate `payload_hash`. 3.
Attempt insert into `events` with `(organization_id, idempotency_key)`.
4. If key conflict: compare stored hash; return existing event if same,
else raise conflict. 5. Insert `outbox` row with
`message_type=event.accepted`, topic and minimal event reference. 6.
Commit. 7. Return `202 Accepted`.

The event must never be acknowledged before the transaction is
committed.

### 8.4 Event routing

One transaction per event: 1. Lock or otherwise claim the event routing
operation idempotently. 2. Query active subscriptions for organization +
event type. 3. Apply filter evaluation. 4. Insert `deliveries` with
generation `1`, `ON CONFLICT DO NOTHING`. 5. For each newly inserted
delivery, insert a `delivery.ready` outbox record. 6. Update event
routing status and counters if stored. 7. Commit. 8. Acknowledge Kafka
offset.

For high fan-out, use bounded chunks. If chunking, introduce a durable
routing progress/cursor state so a crash cannot leave an event marked
complete before all subscriptions are processed.

### 8.5 Delivery attempt lifecycle

Do not hold a DB transaction during outbound HTTP.

**Transaction A --- claim:** - Atomically update eligible delivery to
`IN_PROGRESS`. - Set `lease_owner`, random `lease_token`, and
`lease_expires_at`. - Increment attempt count or reserve the attempt
number atomically. - Insert an `IN_PROGRESS` attempt record. - Commit.

**Outside transaction:** - Load endpoint configuration and secrets. -
Build URL/request. - Acquire token if needed. - Make HTTP call with
strict timeout. - Capture bounded response metadata.

**Transaction B --- finalize:** - Lock delivery and verify current
`lease_token` matches. - Finalize attempt record. - If successful, mark
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

Use a conditional update or `SELECT ... FOR UPDATE SKIP LOCKED` with a
lease token. Example for one delivery:

``` sql
UPDATE deliveries
SET status = 'IN_PROGRESS',
    lease_owner = :worker_id,
    lease_token = :lease_token,
    lease_expires_at = now() + interval '60 seconds',
    attempt_count = attempt_count + 1,
    updated_at = now(),
    version = version + 1
WHERE id = :delivery_id
  AND status IN ('PENDING', 'ENQUEUED', 'RETRY_SCHEDULED')
  AND next_attempt_at <= now()
RETURNING *;
```

A production implementation must ensure a ready Kafka message cannot
bypass `next_attempt_at`; a non-due message should be acknowledged or
deferred without spinning, because the scheduler will emit a new message
when due.

### 9.3 Lease duration

Lease duration must exceed the maximum bounded execution time plus
finalization margin, or be extended by heartbeat. Since token
acquisition plus webhook request can exceed one HTTP timeout, define an
overall attempt deadline and either: - choose a lease larger than the
overall deadline plus margin; or - renew the lease with a
compare-and-set on `lease_token`.

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

The scheduler scans due deliveries with an index-friendly bounded query:

``` sql
SELECT id
FROM deliveries
WHERE status = 'RETRY_SCHEDULED'
  AND next_attempt_at <= now()
ORDER BY next_attempt_at
FOR UPDATE SKIP LOCKED
LIMIT :batch_size;
```

For each selected delivery, in the same transaction: 1. Transition it to
`ENQUEUED` (or the chosen ready state). 2. Insert `delivery.ready`
outbox record. 3. Commit.

Run a separate bounded scan for expired leases. Recover only rows where
`status='IN_PROGRESS' AND lease_expires_at < now()`. Apply a
compare-and-set to prevent racing a live worker heartbeat.

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

-   Encrypt credentials using a managed key or store them in a secret
    manager.
-   Include a config/credential version in cache keys so secret rotation
    invalidates old tokens.
-   Never return credentials from `GET /v1/endpoints`.
-   For update requests, omitted secrets mean unchanged; use explicit
    secret replacement.
-   Redact auth request bodies and responses from application logs.
-   Token request destinations require the same SSRF protections as
    webhook destinations.

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
-   Pause partitions or use bounded executor queues when downstream DB
    capacity is exhausted.
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
background merges, not synchronously at insert time. For correctness-
sensitive queries, use `FINAL` when its cost is acceptable or aggregate
with `argMax(..., version)` grouped by the complete logical key. Do not
assume a normal `SELECT` immediately returns one row per `fact_id`.

Example query returning deduplicated facts for an organization and time
range:

```sql
SELECT
    fact_id,
    argMax(fact_type, version) AS fact_type,
    argMax(status, version) AS status,
    argMax(event_id, version) AS event_id,
    argMax(delivery_id, version) AS delivery_id,
    argMax(endpoint_id, version) AS endpoint_id,
    argMax(occurred_at, version) AS occurred_at,
    argMax(http_status, version) AS http_status,
    argMax(duration_ms, version) AS duration_ms
FROM relayforge_analytics.delivery_facts
WHERE organization_id = {organization_id:UUID}
  AND event_date >= {start_date:Date}
  AND event_date <= {end_date:Date}
GROUP BY fact_id;
```

The API layer must always inject the authenticated organization ID; it
must not trust an organization ID supplied by the caller. Bound time
ranges and query timeouts are required. `argMax` resolves versions of a
single logical fact; it does not make separate fact IDs representing
separate events equivalent.

### 14.4 Optional daily rollup table (deferred)

Do not create an incremental materialized view directly over the raw
fact table for correctness-sensitive counts in v1: duplicate Kafka
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

Initial proposed limits (validate against product requirements): - Event
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
PostgreSQL and populate Redis. On Redis failure, fall back to PostgreSQL
with bounded load protection; if PostgreSQL is unavailable, fail closed
for quota-sensitive write operations rather than bypassing limits.
Invalidate or version-bump the cache when quota configuration changes.
Exact plan values and hard maximums are deployment configuration and
must be defined before production rollout.

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
9. Document 30-day default retention for payloads and delivery
   diagnostics. Automated purge is deferred and not implemented in v1.
10. Document 30-day outbox retention followed by purge. Automated purge
    is deferred and not implemented in v1; unpublished rows are never
    purged.
11. No ordering guarantee in v1.
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

Implementation configuration still required (these do not change the architecture):
- Set concrete quota defaults and hard platform maximums in configuration and tests.
- Select the envelope-encryption algorithm/key provider and document key rotation.
- Select and pin the JSONPath library used only for token-response extraction.
- Measure ClickHouse query-time deduplication cost and record benchmark evidence.
- Automated 30-day retention purge is explicitly deferred; document it and do not imply it runs.

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

The design is based on the RelayForge Technical Specification v3.0 and
High-Level Design v3.0. External technology documentation to consult
during implementation:

-   PostgreSQL: https://www.postgresql.org/docs/
-   Apache Kafka: https://kafka.apache.org/documentation/
-   Spring Boot: https://docs.spring.io/spring-boot/
-   ClickHouse: https://clickhouse.com/docs
-   Redis: https://redis.io/docs/
-   Google Cloud Secret Manager:
    https://cloud.google.com/secret-manager/docs

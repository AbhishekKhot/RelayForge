# RelayForge — Implementation Checklist and Docker Verification Plan

**Basis:** HLD, LLD, PostgreSQL templates in `docs/schema/`, and the planning load profile in HLD §13
**Purpose:** Implement in small, testable increments. Complete A1 and A2 before D0; then finish each dependent checkpoint in Docker before moving on.
**Status convention:** `[ ]` not started · `[x]` implemented and verified · `[!]` blocked or design deviation. Do not mark a task complete because code compiles; complete its stated checkpoint.

## 1. Implementation rules

- [ ] Treat the HLD/LLD as the contract. Record intentional deviations in a design decision note before implementation.
- [ ] Keep PostgreSQL authoritative. Kafka is transport; Redis is an optimization/coordination layer; ClickHouse is eventually consistent analytics.
- [ ] Use Flyway migrations; do not rely on ORM auto-DDL outside disposable tests.
- [ ] Every write path must define transaction boundary, idempotency behavior, organization scope, and retry/crash behavior.
- [ ] Add unit tests with each domain/service implementation and integration tests at each checkpoint below.
- [ ] Follow test-driven development for every feature: write a failing behavior test, implement the feature, then refactor with all required tests green. A feature is complete only when 100% of its applicable test cases pass; this is a pass-rate gate, not a claim of 100% code coverage.
- [ ] Build a separate Docker image for every independently deployable service or worker role. Run each feature's normal functional, integration, security, and failure tests against the Dockerized dependencies before checking it off.
- [ ] Finish all feature checkpoints and the full Docker end-to-end suite before running the normal, peak, or headroom load tests. Performance targets remain unverified until that final stage.
- [ ] Use a local webhook receiver/simulator in Docker. Never test delivery against arbitrary public URLs.
- [ ] All secrets, API keys, tokens, authorization headers, and full credential responses must be excluded from logs and analytics.
- [ ] Use UTC timestamps in persistence and messages. Use bounded request/response sizes and explicit connect/read/overall timeouts.
- [ ] Do not claim a performance target is met until the load-test report records environment, configuration, workload, sample count, throughput, latency percentiles, errors, and bottleneck evidence.

## 2. Docker development environment

### Capacity architecture gate before D0 and Milestone A

- [x] Define HLD §13 storage ownership and transaction boundaries in the LLD: local outboxes, versioned configuration projection and bounded reads, durable payload access, and no cross-service foreign keys.
- [x] Provide service-owned PostgreSQL 17 physical DDL templates and migration order in `docs/schema/`; keep cross-service references out of foreign keys and verify local constraints against PostgreSQL 17.
- [x] Specify stable `(organization_id, idempotency_key)` ingestion shard routing, a 90-day shard-local idempotency registry, versioned event/delivery locators, hot-tenant handling, and restart/recovery semantics.
- [ ] Before enabling bucket moves, test stale-map rejection, source fencing, registry copy verification, old-locator reads, cutover recovery, and concurrent same-key requests on both sides of expiry.
- [x] Define daily `accepted_day` partitions for event/delivery history, composite keys, local foreign keys, cross-service ownership checks, partition pre-creation, the 25-hour physical-deletion grace, and blocked-drop behavior. Keep configuration tables outside event-age purge.
- [x] Specify production primary/standby failover and backup strategy per write shard; require synchronous standby WAL confirmation before ingestion `202`, and a retryable failure when no synchronous standby is available. Do not assign an untested shard count.
- [ ] During Docker failure testing, prove an unavailable synchronous standby blocks ingestion `202`; a commit timeout retries with the same idempotency key; promotion fences the old primary; and an isolated backup/WAL restore recovers committed state.
- [x] Define a separate Docker image and test target for each service and worker role in `deploy/README.md`; Compose is the functional environment, while the final load environment has documented distributed resource categories.

**Gate:** Review the revised architecture and migration plan before creating tables. Do not treat the shared LLD example schema or a single-host Compose run as proof of the HLD capacity target.

### 2.1 Expected Compose services

- [x] `postgres`: PostgreSQL 17 with separate control, ingestion, and delivery databases; health check waits for successful SQL connection.
- [x] `kafka` and its required local dependency/configuration: topics for configuration changes, accepted events, ready deliveries, and analytics facts.
- [x] `redis`: local quota/token-cache service; bounded application behavior during an outage remains with later Redis-using features.
- [x] `clickhouse`: analytics database and `relayforge_analytics.delivery_facts` table.
- [x] `control-plane` and `ingestion`: independently runnable REST application scaffolds; business endpoints remain in later milestones.
- [x] `control-publisher`, `ingestion-publisher`, and `delivery-publisher`: independently runnable outbox publisher scaffolds; publish behavior remains in D1.
- [x] `router`: independently runnable router scaffold; routing behavior remains in D3.
- [x] `delivery-api`: independently runnable API scaffold; query and replay behavior remains in E3/F3.
- [x] `delivery-worker`: independently runnable worker scaffold; lease and webhook behavior remains in E1/E2.
- [x] `retry-scheduler`: independently runnable scheduler scaffold; retry behavior remains in F1.
- [x] `analytics-consumer`: independently runnable consumer scaffold; insert behavior remains in H1.
- [x] `analytics-api`: independently runnable API scaffold; bounded query behavior remains in H3.
- [x] `webhook-simulator`: local endpoints for 2xx, 4xx, 429, 5xx, delay, timeout, and connection reset tests.

Each role has a separate image and test target. Services in one ownership boundary may share source libraries, but their runtime images and replica counts are independent.

### 2.2 Standard commands

Adapt service names to the actual `compose.yaml`; keep equivalent commands in the repository README.

```bash
# Start infrastructure and application
 docker compose up -d --build

# Inspect startup and migrations
 docker compose ps
 docker compose logs --tail=200 ingestion

# Follow asynchronous processing
 docker compose logs -f control-publisher ingestion-publisher delivery-publisher router delivery-worker retry-scheduler analytics-consumer

# Stop only one dependency for failure tests
 docker compose stop redis
 docker compose start redis

# Run a role's Docker functional tests
 docker compose run --rm test-ingestion

# Inspect PostgreSQL state
 docker compose exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"

# Inspect ClickHouse
 docker compose exec clickhouse clickhouse-client --query \
   "SHOW TABLES FROM relayforge_analytics"
```

- [x] Commit `.env.example` with safe local defaults and no real secrets.
- [x] Add readiness checks for dependencies; `depends_on` alone is not a readiness guarantee.
- [x] Add a documented reset command for disposable local data. Clearly warn that it deletes local volumes.
- [x] Document ports, service names, test credentials/bootstrap procedure, and available health API examples in `README.md`.

**Checkpoint D0 — Clean boot (after A2):** Once the application bootstrap and owner migrations exist, `docker compose up -d --build` succeeds from a clean checkout; all required services become healthy; migrations run once; `/health/live` returns `200`; `/health/ready` returns `200` only when required dependencies are available. Restarting the stack does not lose committed PostgreSQL data.

**D0 verification:** [PR #10 CI](https://github.com/AbhishekKhot/RelayForge/actions/runs/40835097981) passed Java 21 `mvn verify` and `scripts/verify-compose.sh` on a clean Docker runner. The script builds all role images, checks service health, topics, ClickHouse schema, three owner Flyway histories, every role's readiness target, dependency outage responses, and committed PostgreSQL data after a stack restart. The same Docker acceptance could not run on the local machine because its Docker daemon was unavailable.

## 3. Milestone A — Project skeleton, database, and identity

### A1. Application structure and configuration

- [x] Create Java 21 / Spring Boot entry points for every deployable role, with shared runtime and HTTP support modules. Add domain packages as their owning milestones are implemented.
- [x] Define configuration binding and validation for database, Kafka, Redis, ClickHouse, API limits, webhook timeout, retry policy, encryption key provider, and trusted provisioning credentials.
- [x] Add request ID middleware; propagate `X-Request-Id` to logs and responses.
- [x] Add consistent error response DTO and validation errors. Do not return stack traces or internal dependency details.

**Touches:** API bootstrap/configuration, shared error handling, logging. No domain tables yet.  
**Checkpoint A1:** App builds in Docker; malformed requests return a stable 4xx schema; unexpected exceptions return sanitized 5xx; each request has a correlation ID.

### A2. PostgreSQL migrations and schema constraints

- [x] Add Flyway migrations for each owner's tables using the PostgreSQL 17 templates in `docs/schema/` and the LLD's logical fields where compatible.
- [x] Add foreign keys, uniqueness constraints, status checks where appropriate, and indexes for route lookup, due work, expired leases, organization-scoped queries, and outbox claims.
- [x] Add migration tests against PostgreSQL 17.11; pin Compose to the same minor release at D0.
- [x] Add composite organization/cohort ownership keys and local foreign keys, and reject cross-organization references in integration tests. Cross-service API/event boundary validation remains with A3, C1, and D3, where those boundaries are implemented.
- [x] Test that daily partition keys are present in required unique/primary keys, future partitions are pre-created, and partition-boundary inserts and queries work.
- [x] Keep A2 free of organization-owned application queries. Authenticated tenant query scoping remains a required test in A3, C2, E3, F3, and H3 as those queries are added.

**Touches:** Flyway, all listed PostgreSQL tables.  
**Checkpoint A2:** Empty-volume boot creates the schema; a second boot does not reapply migrations; invalid foreign keys and duplicate unique keys are rejected by PostgreSQL.

**A2 verification:** Java 21 `mvn verify` passed with 21 tests, including Spring Boot startup migrations and PostgreSQL 17.11 Testcontainers checks for repeat migration, ownership, uniqueness, and partition boundaries. D0 Compose and ongoing partition maintenance remain separate checkpoints.

### A3. Organization provisioning and API keys

- [x] Implement protected `POST /v1/organizations` for trusted provisioning only; create `organizations` with plan and `quota_config` defaults/explicit overrides.
- [x] Issue a high-entropy API key; persist `key_id`, organization ID, scopes, timestamps, revocation/expiry fields, and only a secure hash/digest of the secret in `api_keys`.
- [x] Return the plaintext key once. Never expose the stored hash or return the secret on later reads.
- [x] Implement authentication middleware: resolve key ID, verify secret in constant time, enforce revocation/expiry/scope, update `last_used_at` safely, and establish the authenticated organization context.
- [x] Implement `POST /v1/users` for trusted provisioning with existing `organization_id`, email, display name, and metadata. Enforce `UNIQUE (organization_id, email)`.
- [x] Add key rotation/revocation behavior and an audit record for provisioning/security-sensitive changes.

**Touches:** `organizations`, `api_keys`, `users`, `audit_records`; `OrganizationController`, API-key filter/provider, provisioning authorization. APIs: `POST /v1/organizations`, `POST /v1/users` plus internal/admin key revocation route if exposed.  
**Checkpoint A3:** Provision organization → receive key once → call a protected test endpoint successfully → wrong key gets `401` → revoked/expired key gets `401` → insufficient scope gets `403` → organization A cannot read organization B data. Verify database contains no plaintext key.

**A3 verification:** [PR #11 CI](https://github.com/AbhishekKhot/RelayForge/actions/runs/40851999214) passed Java 21 `mvn verify` and `scripts/verify-compose.sh`. PostgreSQL integration tests cover trusted provisioning, key digest storage and one-time response, scope and expiry enforcement, rotation/revocation and audit, user uniqueness, quota validation, organization isolation, and safe recovery after an uncertain provisioning response. Control-plane authentication currently guards its own `/v1` routes; authentication at other service boundaries is implemented with their later API milestones.

## 4. Milestone B — Endpoint registration and subscription routing config

### B1. Endpoint request validation and SSRF protection

- [x] Implement URL composition and validation for base URL, path, query params, and headers; allow only supported schemes (HTTPS by default; HTTP only in explicit local/dev policy).
- [x] Reject loopback, private, link-local, multicast, reserved, and cloud metadata IP ranges after DNS resolution; defend against DNS rebinding by validating resolved targets at registration and immediately before delivery.
- [x] Disable redirects in the outbound HTTP client. Reject unsupported URL forms, embedded credentials, unsafe headers, and invalid timeouts/concurrency/attempt limits.
- [x] Keep outbound networking behind a single policy-enforcing client so callers cannot bypass SSRF checks.

**Touches:** endpoint validation/security module; `endpoints` only after validation succeeds. API: `POST /v1/endpoints`.  
**Checkpoint B1:** Local simulator URL works under an explicit dev allowlist; metadata/private IP URLs are rejected; redirect response is not followed; malformed URL/header/query configuration gets a sanitized 4xx.

**B1 verification:** [PR #12 CI](https://github.com/AbhishekKhot/RelayForge/actions/runs/41007974354) passed Java 21 `mvn verify` and `scripts/verify-compose.sh`. Tests cover public HTTPS URL composition, blocked private and reserved DNS answers, a rebinding answer before send, disabled redirects, sanitized registration errors, atomic endpoint/subscription inserts, and the `.env`-configured local simulator. B1 supports `authentication.type=NONE`; credential modes remain B2. The outbound client is ready for delivery worker integration in the later delivery milestone.

### B2. Authentication configuration and encryption

- [x] Implement allowlisted auth types: none, static API key/header, Bearer token, Basic auth, and declarative token exchange/client credentials per LLD.
- [x] Encrypt sensitive auth config and signing secrets with application-level envelope encryption or store a secret reference. Keep encryption keys outside source control; define local-only key injection and rotation procedure.
- [x] Validate token URL, method, content type, allowed request fields, token extraction JSONPath, expiry field, and credential references. No arbitrary scripts, templates, or event-payload interpolation.
- [x] Ensure DTOs and entity `toString()` methods never expose ciphertext, plaintext credentials, or token response bodies.

**Touches:** `endpoints.auth_type`, `auth_config_ciphertext`, `auth_secret_ref`, `signing_secret_ciphertext`, endpoint auth services.  
**Checkpoint B2:** Create endpoint with each supported static auth type; read endpoint back and verify secret material is redacted. A wrong/missing encryption key prevents startup or readiness rather than silently storing plaintext.

**B2 verification:** [PR #13 CI](https://github.com/AbhishekKhot/RelayForge/actions/runs/41042893612) passed Java 21 `mvn verify` and Compose acceptance. Tests cover supported auth modes, token URL validation, encrypted secret storage and redaction, and startup rejection with a wrong or missing key.

### B3. Atomic endpoint + subscriptions registration

- [x] Implement `POST /v1/endpoints` accepting `event_types` array plus destination/auth settings.
- [x] In one PostgreSQL transaction insert the endpoint and all `subscriptions` rows; reject duplicate/invalid event types and roll back the whole transaction on any failure.
- [x] Implement `GET /v1/endpoints`, `GET /v1/endpoints/{endpoint_id}`, `PATCH /v1/endpoints/{endpoint_id}`, `GET /v1/subscriptions`; all list/query APIs use organization scope and bounded cursor pagination.
- [x] Implement `POST /v1/endpoints/{endpoint_id}/rotate-secret` for RelayForge's outbound signing secret; audit the action and return the new secret once only if the contract requires it.
- [x] Ensure endpoint disable/update increments `version` and commits a secret-free configuration change to the control outbox.
- [ ] Ensure delivery workers observe the latest configuration before sending. This is verified with the D3 worker/projection implementation.

**Touches:** `endpoints`, `subscriptions`, `audit_records`; `EndpointController`, endpoint/subscription services.  
**Checkpoint B3:** Register one endpoint with two event types → verify one endpoint and two subscription rows. Submit an invalid event type among valid values → verify no endpoint or subscription rows were committed. Cross-organization endpoint access returns not-found/forbidden per API contract.

**B3 verification:** [PR #16 CI](https://github.com/AbhishekKhot/RelayForge/actions/runs/41176049057) passed Java 21 `mvn verify`, clean Compose acceptance including endpoint management, and synchronous-ingestion HA acceptance. Tests cover tenant-scoped pagination, optimistic updates, redacted one-time secret rotation, encryption AAD versions, and atomic audit/configuration outbox writes. Worker observation remains the D3 integration gate.

## 5. Milestone C — Durable event ingestion

### C1. Publish API, validation, and idempotency

- [x] Implement `POST /v1/events` with `event_type`, JSON payload, optional `occurred_at`, schema version, and required `Idempotency-Key` header as defined in LLD.
- [x] Validate payload size/depth, event type, timestamps, JSON structure, and organization status/quota before writing.
- [x] In one ingestion-shard transaction claim the registry key, insert `events`, and write the `ingestion_outbox` message `event.accepted.v1`; return `202` only after synchronous standby commit confirmation.
- [x] Route `(organization_id, idempotency_key)` consistently to one ingestion shard; commit its 90-day registry entry, event, and outbox atomically. Return the original event for an identical request and conflict for a changed request; test key reuse only after expiry.
- [x] Ensure a PostgreSQL error returns non-2xx and no acceptance claim. Do not publish directly to Kafka from the API transaction.

**Touches:** `bucket_fence`, `idempotency_registry`, `events`, `ingestion_outbox`; `EventController`, `EventIngestionService`, idempotency/hash utility, quota service. API: `POST /v1/events`.
**Checkpoint C1:** Publish an event → response is `202` with `event_id`; verify event and pending outbox row exist. Repeat same key/body → same event ID and no second event. Same key/different body → conflict. Stop PostgreSQL → request fails and does not return `202`.

**C1 verification:** [PR #14 CI](https://github.com/AbhishekKhot/RelayForge/actions/runs/41043586973) passed Java 21 `mvn verify`, clean Compose boot and recovery, and the synchronous-ingestion HA checkpoint. Tests cover JSON bounds, canonical request hashing, tenant quota, atomic event/outbox/idempotency writes, same-key replay and conflict, registry expiry, standby loss, and recovery.

### C2. Event query and tenant boundary

- [x] Implement `GET /v1/events/{event_id}` by resolving the versioned locator on ingestion and composing a bounded delivery-owned progress/summary read; return only data belonging to the authenticated organization.
- [x] Apply bounded payload response size and avoid leaking internal ciphertext or internal outbox payloads.
- [x] Add integration tests for guessed UUIDs across organizations and invalid IDs.

**Touches:** `events`, `deliveries`; `EventQueryService`, `EventController`. API: `GET /v1/events/{event_id}`.  
**Checkpoint C2:** Event owner can query acceptance; another organization cannot distinguish or read the event. Before routing is implemented, the delivery summary is pending; after D3 it reflects current counts.

**C2 verification:** [PR #15 CI](https://github.com/AbhishekKhot/RelayForge/actions/runs/41176119743) passed Java 21 `mvn verify`, base Compose acceptance, and synchronous-ingestion HA acceptance with owner and cross-tenant event reads. Tests cover invalid locators, bounded payloads, delivery summary counts, internal credential checks, and unavailable delivery responses.

## 6. Milestone D — Transactional outbox, Kafka, and routing

### D1. Outbox claim/publish/mark algorithm

- [x] Implement bounded batch claims using row locking/leases (`FOR UPDATE SKIP LOCKED` or the LLD-equivalent), claim token, claim expiry, attempt count, and `available_at`.
- [x] Publish each outbox row to the configured Kafka topic with stable message ID, schema version, correlation ID, organization ID, aggregate ID, and partition key.
- [x] Mark published only after Kafka acknowledges. If the process crashes after Kafka publish but before DB marking, republish is expected; consumers must be idempotent.
- [x] Back off transient broker errors; retain diagnostics without secret data. Never delete unpublished rows as a recovery strategy.

**Touches:** `control_outbox`, `ingestion_outbox`, `delivery_outbox`; each owner's publisher, Kafka producer configuration, topic setup.
**Checkpoint D1:** Publish event while publisher is stopped → outbox remains pending. Start publisher → Kafka receives message and row becomes published. Kill publisher after publish but before marking → duplicate Kafka message is tolerated and no data is lost.

**D1 verification:** [PR #17 CI](https://github.com/AbhishekKhot/RelayForge/actions/runs/41182283541) passed Java 21 `mvn verify`, clean Compose control/delivery publication checks, and ingestion HA publication. PostgreSQL tests cover lock contention, lease recovery, stale tokens, retries, and bounded claims. Publisher tests cover Kafka acknowledgement before marking, stable replay envelopes, bounded backoff, and sanitized failures. Consumer deduplication remains D2.

### D2. Topic contracts and consumer resilience

- [ ] Create/configure configuration-change, accepted-event, delivery-ready, and analytics-fact topics with the keys, 24-hour initial retention, local replication settings, and production durability policy in `docs/decisions/kafka-transport.md`; set production partition counts from measured throughput.
- [ ] Implement versioned message envelope and schema validation. Reject/quarantine malformed messages without infinite retry loops.
- [ ] Commit consumer offsets only after the database transaction or ClickHouse insert succeeds. Configure bounded retry and a quarantine/dead-letter path for poison messages.
- [ ] Make consumer handlers idempotent using stable event/delivery/fact identifiers and database uniqueness constraints.
- [ ] Keep polling while slow HTTP requests execute in a bounded pool; pause/resume partitions under backpressure and commit only contiguous completed offsets per partition. Test `max.poll.interval.ms` and rebalance recovery.

**Touches:** Kafka topic/config, message DTOs/serializers, all consumers.  
**Checkpoint D2:** Re-send the same valid message twice and verify no duplicate logical records. Send malformed envelope and verify it is observable/quarantined, not silently discarded or retried forever.

### D3. Event router and logical delivery creation

- [ ] Apply secret-free configuration snapshots to delivery-owned endpoint/subscription projections and contiguous per-organization revision state before routing becomes ready.
- [ ] Consume `event.accepted.v1`; verify the organization and event locator through ingestion, then resolve active endpoints/subscriptions by `(organization_id, event_type)` from the ready projection.
- [ ] Persist matching endpoint/subscription targets once with `routing_progress`; later chunks use those targets so a duplicate message or config change cannot alter fan-out.
- [ ] In bounded local transactions create one `deliveries` and `delivery_work` row per target plus a `delivery.ready.v1` outbox row. Use cohort-aware uniqueness and stable deduplication keys; finish progress only after every target is materialized.
- [ ] Define zero-match behavior: event remains accepted/processed with zero deliveries and observable routing outcome.
- [ ] Commit Kafka offset after durable routing progress; recovery resumes unfinished chunks without recalculating targets.

**Touches:** `endpoint_projection`, `subscription_projection`, `projection_state`, `routing_targets`, `routing_progress`, `deliveries`, `delivery_work`, `delivery_outbox`; `EventRoutingConsumer`, `RoutingService`.
**Checkpoint D3:** Event with two matching endpoints creates exactly two deliveries and two delivery-ready outbox rows. Event with no matching subscription creates none. Duplicate accepted-event message creates no additional generation-1 deliveries.

## 7. Milestone E — Webhook delivery and history

### E1. Lease-safe delivery claiming

- [ ] Claim due `delivery_work` atomically with lease owner, random lease token, expiry, and version increment; update the matching day-partitioned delivery row in the same transaction.
- [ ] Increment `claim_count` on ownership claim; reserve `attempt_count` and insert an HTTP attempt only immediately before each destination request. Authentication failures create only authentication attempts.
- [ ] Reject stale workers when lease token/version no longer matches. Renew a 60-second lease by compare-and-set during the 240-second processing deadline; stop sending if renewal fails.
- [ ] Recover expired `IN_PROGRESS` leases and return them to eligible work without creating duplicate logical deliveries.
- [ ] Enforce endpoint-level concurrency and platform-wide bounded worker concurrency.

**Touches:** `delivery_work`, `deliveries`; `DeliveryClaimRepository`, `DeliveryWorker`, `LeaseRecoveryService`.
**Checkpoint E1:** Two workers compete for one due row; only one claims it. Kill owner while in progress; after lease expiry, recovery requeues it. A stale worker cannot overwrite the new owner's state. A 90-second token exchange followed by a slow webhook does not lose a renewed lease.

### E2. Request construction, signatures, and outbound HTTP

- [ ] Build payload envelope and headers per LLD, including stable `delivery_id`, `event_id`, event type, attempt number, timestamp, and signature metadata.
- [ ] Apply configured auth strategy and HMAC signature; canonicalize bytes consistently so receiver verification is deterministic.
- [ ] Apply DNS/IP SSRF checks immediately before connect, disable redirects, enforce connect/read/overall timeouts, response-size limits, and allowed headers.
- [ ] Treat any 2xx as success. Capture only bounded/sanitized response excerpts; never persist credentials or unbounded bodies.

**Touches:** bounded control-plane endpoint read, ingestion payload read, `delivery_work`, `deliveries`, `delivery_attempts`; `OutboundRequestFactory`, `WebhookHttpClient`, signature/auth strategy.
**Checkpoint E2:** Simulator receives a valid signed request; signature verification passes; `delivery_id` is stable across attempts. A 2xx succeeds; redirect is not followed; slow receiver times out within configured bounds.

### E3. Attempt history and query APIs

- [ ] Insert `delivery_attempts` at attempt start/finish with attempt number, timestamps, outcome, HTTP status, duration, bounded error code, and sanitized excerpt.
- [ ] Reserve each HTTP attempt before sending, including a controlled second request after a 401; finalize status, last error, completion timestamp, and analytics outbox after the outcome. A crash between reservation and send leaves an unknown/aborted attempt and consumes one slot.
- [ ] Implement `GET /v1/deliveries`, `GET /v1/deliveries/{delivery_id}`, and stable cursor pagination with organization scope and filters.
- [ ] Include attempt history in delivery detail; do not query ClickHouse for authoritative state.

**Touches:** `delivery_work`, `deliveries`, `delivery_attempts`, `delivery_outbox`, delivery audit as applicable; `DeliveryQueryService`, `DeliveryController`.
**Checkpoint E3:** A successful simulated delivery appears as `SUCCEEDED` with one completed attempt and HTTP status. Search filters/pagination are stable; cross-organization reads are blocked.

## 8. Milestone F — Retries, dead-lettering, and replay

### F1. Failure classification and retry schedule

- [ ] Implement explicit classification: 2xx success; 408/429, transient network failures, and 5xx retryable; other 4xx terminal by default; configuration/security failures fail fast.
- [ ] Implement bounded exponential backoff with full jitter, maximum attempts, maximum retry horizon, and valid `Retry-After` handling bounded by policy.
- [ ] Persist `next_attempt_at` and `RETRY_SCHEDULED` in both `delivery_work` and `deliveries` in one transaction; do not sleep a worker thread while waiting for the retry delay.
- [ ] Ensure state change and analytics-fact outbox insert are atomic.

**Touches:** `delivery_work`, `deliveries`, `delivery_attempts`, `delivery_outbox`; `RetryPolicy`, `DeliveryOutcomeService`.
**Checkpoint F1:** Simulator 500 → delivery becomes retry-scheduled with future `next_attempt_at`; 429 with `Retry-After` respects the bound; 400 becomes terminal; 200 becomes succeeded. Test retry exhaustion results in `DEAD_LETTERED`.

### F2. Due-work scanner and crash recovery

- [ ] Scan indexed due rows in bounded batches and create delivery-ready outbox work safely; guard against duplicate scheduling.
- [ ] Recover expired leases and handle worker crashes at each boundary: before HTTP, during HTTP, after receiver success but before result commit.
- [ ] Make at-least-once duplicate delivery explicit: the receiver can observe a duplicate if RelayForge cannot know whether the first request was processed.
- [ ] Add metrics for due backlog, oldest due age, expired leases, retry exhaustion, and outbox age.

**Touches:** `delivery_work`, `deliveries`, `delivery_outbox`; `RetryScheduler`, lease recovery, publisher.
**Checkpoint F2:** Stop worker and scheduler, create retry-due work, restart services, and verify work resumes. Simulate post-receiver/pre-commit crash and verify the same logical delivery is retried with the same `delivery_id`, while attempt history remains correct.

### F3. Replay and audit

- [ ] Implement `POST /v1/deliveries/{delivery_id}/replay`; verify ownership and replay eligibility.
- [ ] Create a new delivery row with new `delivery_id`, incremented generation, and linkage to the original; preserve original delivery and attempt history.
- [ ] Insert `replay_requests`, `delivery_audit_records`, `delivery_work`, and delivery-ready outbox in one transaction. Require the replay idempotency key; return the original replay for a matching request and conflict for a changed request.
- [ ] Return `202` only after replay transaction commits.

**Touches:** `deliveries`, `delivery_work`, `replay_requests`, `delivery_audit_records`, `delivery_outbox`. API: `POST /v1/deliveries/{delivery_id}/replay`.
**Checkpoint F3:** Replay a dead-lettered delivery → original remains unchanged, new generation is queued with a different ID, audit/replay rows exist, and new webhook request uses the new ID. Unauthorized replay is rejected.

## 9. Milestone G — Dynamic token authentication

### G1. Token exchange strategy

- [ ] Implement a typed strategy interface with supported static auth and token-exchange implementations; avoid condition-heavy code in the worker.
- [ ] Execute token requests using validated allowlisted fields, separate token endpoint timeouts, bounded response size, and JSONPath only for token-response extraction.
- [ ] Record `authentication_attempts` separately from webhook `delivery_attempts` so auth endpoint failures are not reported as webhook response failures.
- [ ] Fail fast on invalid credentials/configuration responses; retry transient network/408/429/5xx errors with exponential backoff and jitter, initial attempt plus up to five retries, bounded by 90 seconds total.

**Touches:** `endpoints`, `authentication_attempts`, `deliveries`; `TokenExchangeService`, auth strategy, token response parser.  
**Checkpoint G1:** Simulator token endpoint issues token; worker obtains it and calls webhook. Malformed response/invalid config fails fast; transient 500/429 retries within deadline without incrementing outbound `attempt_count`; secrets and token body do not appear in logs.

### G2. Token cache and stampede control

- [ ] Cache by organization ID, endpoint ID, auth config version, and credential version; never key only by endpoint URL or organization.
- [ ] Add TTL jitter, local single-flight, per-token-key distributed lock, cache re-check after lock acquisition, and early refresh.
- [ ] Bound lock wait and define behavior if Redis is unavailable; avoid a synchronized cache miss causing unbounded auth traffic.
- [ ] Invalidate/rotate cache entries when endpoint auth config or credentials change.

**Touches:** Redis token cache/lock keys, endpoint version fields, auth provider.  
**Checkpoint G2:** Concurrent deliveries to one endpoint cause one token acquisition under normal cache-miss conditions. Different endpoints/credential versions do not share tokens. Redis stop does not hang workers or produce unbounded retries.

## 10. Milestone H — Analytics pipeline and ClickHouse

### H1. Analytics outbox facts

- [ ] Emit analytics facts in the same PostgreSQL transaction as the delivery/event state change that produced them.
- [ ] Assign stable `fact_id`; keep `version` stable for identical redeliveries and increment only for a logical correction. Derive immutable `event_date` from `occurred_at` UTC.
- [ ] Include bounded, sanitized fields only; never send payloads, API keys, auth config, tokens, or raw error text to ClickHouse.

**Touches:** `outbox`; analytics fact builder and state transition services.  
**Checkpoint H1:** State transition and analytics outbox row commit or roll back together. Duplicate processing does not create a new logical fact ID.

### H2. ClickHouse DDL and ingestion

- [ ] Apply `CREATE DATABASE IF NOT EXISTS relayforge_analytics` and `delivery_facts` DDL from the LLD using `ReplacingMergeTree(version)`, `PARTITION BY toYYYYMM(event_date)`, and `ORDER BY (organization_id, event_date, fact_id)`.
- [ ] Implement analytics consumer validation, insert, and Kafka offset commit only after ClickHouse acknowledges insert.
- [ ] Make duplicate inserts safe with stable `fact_id`/`version`; use `FINAL` for correctness-sensitive queries. Test null corrections, duplicate facts, and equal-version content invariants; benchmark query cost.
- [ ] Add bounded retries and observability for ClickHouse unavailability. Analytics lag must not block event acceptance or webhook delivery.

**Touches:** `relayforge_analytics.delivery_facts`, analytics Kafka topic, `AnalyticsConsumer`.  
**Checkpoint H2:** Valid fact appears in ClickHouse; replaying the same Kafka message does not change deduplicated query results; stopping ClickHouse pauses/backs up analytics consumption but delivery continues; consumer resumes after ClickHouse restarts.

### H3. Analytics API

- [ ] Implement `GET /v1/analytics/overview?from=&to=` and `GET /v1/analytics/deliveries/timeseries?from=&to=&interval=&endpoint_id=`.
- [ ] Require organization scope, validate bounded time ranges and supported intervals, and apply query timeouts/row limits.
- [ ] Return freshness metadata such as `data_as_of`/latest ingested timestamp. Do not present analytics as real-time authoritative delivery state.
- [ ] Keep individual event/delivery detail endpoints on PostgreSQL.

**Touches:** ClickHouse, analytics query service/controllers.  
**Checkpoint H3:** Analytics totals match a controlled test dataset after accounting for freshness and deduplication; cross-organization filters cannot access another organization's facts; invalid/large ranges are rejected or bounded.

## 11. Milestone I — Quotas, security, and operations

### I1. Quota enforcement and Redis failure behavior

- [ ] Store per-organization quota overrides in `organizations.quota_config`; apply plan defaults when overrides are absent.
- [ ] Cache quota config in Redis with expiry/invalidation; never treat Redis as authoritative quota storage.
- [ ] Define bounded fallback and hard platform maximums. Fail closed for quota-sensitive writes when PostgreSQL is unavailable; do not let Redis outage disable all limits.
- [ ] Enforce request rate, event payload size, active endpoint count, delivery concurrency, and replay limits as configured.

**Touches:** `organizations`, Redis quota keys, API filter, ingestion/endpoint/replay services.  
**Checkpoint I1:** Exceed each configured quota and verify stable `429`/quota error; change persisted quota and verify cache refresh; stop Redis and confirm safe bounded fallback; stop PostgreSQL and confirm quota-sensitive writes fail closed.

### I2. Security regression suite

- [ ] Verify organization isolation for every endpoint/query/replay/analytics API.
- [ ] Test SSRF payloads, DNS rebinding strategy, redirects, metadata IPs, unsafe headers, oversized payloads, and response-body limits.
- [ ] Verify API keys are hashed, auth config is encrypted/referenced, secrets are redacted, and logs/errors/analytics contain no credentials.
- [ ] Verify CORS/management endpoints are not overly permissive; health endpoints reveal only minimal status.
- [ ] Add dependency scanning and secret scanning to CI.

**Touches:** API security, endpoint outbound client, logging, all org-scoped repositories.  
**Checkpoint I2:** Security test suite passes in CI; seeded secret canaries do not appear in application logs, ClickHouse, or error responses.

### I3. Observability and health

- [ ] Expose `/health/live` and `/health/ready`; readiness returns `503` when required dependencies prevent safe operation.
- [ ] Add metrics for API rates/errors/latency, outbox pending count/oldest age, Kafka lag, routing fan-out, delivery backlog/oldest age, attempts/outcomes, retry/dead-letter counts, expired leases, token cache hit/miss, auth failures, ClickHouse lag, and analytics query duration.
- [ ] Emit structured logs with request ID, organization ID, event ID, delivery ID, attempt number, and worker name where applicable; never log secrets or full payloads by default.
- [ ] Add actionable alerts/runbook notes for stuck outbox, high Kafka lag, growing due backlog, retry exhaustion, lease recovery spikes, and analytics lag.

**Touches:** actuator/health, metrics registry, structured logging, deployment config.  
**Checkpoint I3:** Stop each dependency in turn and verify expected readiness/fallback behavior and visible metrics; verify one event can be traced from API request through outbox, Kafka, routing, delivery, and analytics by IDs.

### I4. PostgreSQL event-history lifecycle and storage growth

- [ ] Implement restart-safe 90-day cleanup by event acceptance cohort for `events`, `deliveries`, `delivery_attempts`, `authentication_attempts`, and `replay_requests`. Pre-create daily partitions, check dependencies, detach/drop fully expired days, and handle the partial cutoff day with a documented method.
- [ ] Retain `organizations`, `api_keys`, `users`, `endpoints`, and `subscriptions` regardless of event age. Keep account/configuration audit records under a separate policy; align event-specific audit details with the event lifecycle.
- [ ] Remove only confirmed published outbox rows after recovery checks. Alert on pending/in-flight work older than its cohort; recover or explicitly terminate it before any partition drop.
- [ ] Verify shard-local idempotency uniqueness throughout 90 days and reuse after expiry. Test cross-day delivery and replay references, expired replay rejection, stale locators, and tenant isolation.
- [ ] Measure bytes per event, delivery, attempt, and outbox row, including indexes and WAL; monitor oldest eligible row, cleanup lag, disk growth, and vacuum health.

**Checkpoint I4:** Seed aged terminal, active, and unpublished records across day boundaries. Cleanup removes only eligible history, survives restart, retains configuration, and refuses unsafe partition drops. Sustained cleanup throughput is measured during final J3 load tests.

## 12. Milestone J — End-to-end Docker acceptance suite

### J1. Happy-path scenario

- [ ] Provision organization and API key through trusted local provisioning flow.
- [ ] Register endpoint to `webhook-simulator` for `order.created`.
- [ ] Publish `order.created` event with idempotency key.
- [ ] Verify `202`, event row, event outbox, Kafka accepted message, delivery row, delivery-ready outbox, receiver request, successful attempt, final delivery state, and analytics fact.
- [ ] Query event, delivery details, delivery list, analytics overview, and time series.

**Checkpoint J1:** One documented command/script runs the full flow and asserts every durable state transition; it exits non-zero on any failed assertion.

### J2. Failure and duplicate scenarios

- [ ] Duplicate API request with same idempotency key and same body.
- [ ] Same idempotency key with different body.
- [ ] Duplicate Kafka accepted-event and delivery-ready messages.
- [ ] Publisher crash after Kafka ack and before outbox mark.
- [ ] Worker crash before send, during send, and after receiver success before result commit.
- [ ] Receiver returns 400, 408, 429 with `Retry-After`, 500, timeout, reset, and 2xx.
- [ ] Expired lease recovery and stale-worker update attempt.
- [ ] Token endpoint invalid response, timeout, 429/500, and Redis outage.
- [ ] ClickHouse outage and recovery.
- [ ] Replay of terminal delivery and organization-scope violation.

**Checkpoint J2:** Each scenario has a deterministic assertion over HTTP response, PostgreSQL rows/state, Kafka/outbox state, receiver request count, and analytics outcome. Any expected duplicate external request is explicitly documented as part of at-least-once semantics.

### J3. Load and benchmark evidence

- [ ] Create repeatable producer and receiver load generators with configurable event size, endpoint fan-out, receiver latency, and failure percentage.
- [ ] Only after J1/J2 and every feature checkpoint pass, plan and run HLD §13 profiles: 5,000 events/s normal, 10,000 events/s peak, and 15,000 events/s headroom. Define run durations and acceptance thresholds at that stage. Record 3× fan-out and 1.1 attempts/delivery as assumptions, then report actual values.
- [ ] Measure at minimum: ingestion throughput/latency, routing throughput, delivery throughput, backlog drain rate, retry behavior, PostgreSQL connection pool use, Kafka consumer lag, Redis/token-cache hit ratio, and ClickHouse ingest/query latency.
- [ ] Report accepted-event p95/p99, delivery lag p95/p99, backlog size and drain time, disk/WAL growth, database CPU/locks, and failure counts for every profile. Treat the profiles as goals until measured.
- [ ] Save raw result files, environment versions, Docker resource limits, command lines, configuration, percentiles, error counts, and bottleneck analysis in `docs/benchmarks/`.
- [ ] Repeat runs and report variance; do not claim “50K load” without specifying whether that means events, requests, deliveries, or concurrent clients and the achieved duration/latency/error rate.

**Checkpoint J3:** A reproducible benchmark report exists; failures and bottlenecks are explained; all claimed numbers can be regenerated from checked-in scripts/configuration.

## 13. API smoke-test sequence

The values below are placeholders. Use the local bootstrap mechanism and simulator URL defined in README. Keep API keys out of shell history in shared environments.

```bash
export BASE_URL=http://localhost:8080
export RF_API_KEY='<key-returned-by-local-provisioning>'
export IDEMPOTENCY_KEY="smoke-$(date +%s)"

# Health
curl -i "$BASE_URL/health/live"
curl -i "$BASE_URL/health/ready"

# Register an endpoint. Replace the simulator URL with the Compose-reachable URL.
curl -i -X POST "$BASE_URL/v1/endpoints" \
  -H "Authorization: Bearer $RF_API_KEY" \
  -H 'Content-Type: application/json' \
  -H 'X-Request-Id: smoke-endpoint-001' \
  -d '{
    "name":"local-order-receiver",
    "destination":{
      "base_url":"http://webhook-simulator:8090",
      "path":"/webhooks/orders",
      "query_params":{},
      "headers":{"Content-Type":"application/json"},
      "metadata":{"environment":"local"}
    },
    "authentication":{"type":"NONE"},
    "event_types":["order.created"],
    "delivery_policy":{"timeout_ms":3000,"max_attempts":5,"max_concurrency":2}
  }'

# Publish; API contract returns 202 only after durable event + outbox commit.
curl -i -X POST "$BASE_URL/v1/events" \
  -H "Authorization: Bearer $RF_API_KEY" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $IDEMPOTENCY_KEY" \
  -d '{"event_type":"order.created","schema_version":1,"data":{"order_id":"ord-smoke-1","amount":1250}}'

# After capturing returned IDs:
curl -i "$BASE_URL/v1/events/<event_id>" -H "Authorization: Bearer $RF_API_KEY"
curl -i "$BASE_URL/v1/deliveries?limit=20" -H "Authorization: Bearer $RF_API_KEY"
curl -i "$BASE_URL/v1/deliveries/<delivery_id>" -H "Authorization: Bearer $RF_API_KEY"
curl -i "$BASE_URL/v1/analytics/overview?from=<UTC_ISO>&to=<UTC_ISO>" \
  -H "Authorization: Bearer $RF_API_KEY"
```

Notes:
- Organization creation is a trusted provisioning operation; do not expose it as anonymous self-service just to simplify a smoke test.
- The Compose DNS name `webhook-simulator` is resolvable from containers, not necessarily from the host. If SSRF policy disallows HTTP in development, configure an explicit local-only allowlist rather than weakening production defaults.
- Capture IDs from actual responses; do not assume IDs or asynchronous completion timing.

## 14. Release gate / definition of done

- [ ] Every checkpoint D0 through J3, including I4, is complete, with commands/results recorded.
- [ ] API contract tests, migration tests, unit tests, integration tests, security tests, and failure-injection tests pass in CI.
- [ ] Clean Docker boot and restart are reproducible from README.
- [ ] Organization isolation, idempotency, transactional outbox, duplicate-safe consumers, lease recovery, retries, replay, and analytics deduplication are demonstrated by tests.
- [ ] ClickHouse DDL matches the LLD. Analytics responses communicate freshness; PostgreSQL remains authoritative for individual event/delivery state.
- [ ] A restart-safe 90-day PostgreSQL event-history lifecycle is tested against partition keys, references, replay, tenant scope, and live work. Configuration remains; unpublished outbox rows are never purged by age.
- [ ] Secrets/SSRF/quota controls are tested. No plaintext API keys or credentials are persisted or logged.
- [ ] Benchmark report includes measured evidence and no unsupported throughput claims.
- [ ] README, `.env.example`, Compose configuration, API examples, test scripts, runbooks, and known limitations are current.

## 15. Deferred items — do not silently treat as implemented

- [ ] Production KMS/secret-manager integration and full key-rotation operations if local envelope encryption is the first implementation.
- [ ] ClickHouse replicated/distributed topology; current DDL is a local/single-node baseline.
- [ ] Daily aggregate/rollup tables; deferred until raw fact query patterns and duplicate-safe aggregation are benchmarked.
- [ ] Multi-region active-active, guaranteed ordering, exactly-once external effects, arbitrary scripts/transforms, and dedicated customer infrastructure are out of scope for v1.

# RelayForge — Project Discussion and Follow-up Questions

Interview preparation notes based on the current RelayForge design decisions. Answers use plain technical English and distinguish the current design from possible future changes.

## How to answer project questions

Use this four-part structure:
1. **Current design:** what RelayForge does today.
2. **Problem:** what can fail or become slow.
3. **Change:** what you would implement and why.
4. **Trade-off:** what extra cost or complexity the change introduces.

Do not claim a future design, feature, or benchmark is already implemented. Say “in the current design” or “I would add this if the requirement appears.”

---

## 1. Product and architecture

### Q1. Explain RelayForge in one minute.
RelayForge accepts events from an organization and delivers them to registered webhook endpoints. The API stores the event and an outbox record in PostgreSQL in one transaction, then returns `202 Accepted`. An outbox publisher sends the event to Kafka. A routing worker finds endpoints subscribed to the event type and creates delivery jobs. Delivery workers call the external endpoints, record attempts, and schedule retries when appropriate. ClickHouse stores analytics facts; PostgreSQL remains the source of truth for event and delivery state.

The main design goal is to accept events reliably without making the caller wait for every external webhook to finish.

### Q2. Why not call all webhook endpoints directly inside the publish API?
The API would have to wait for slow or unavailable external servers. One endpoint taking 20 seconds could consume a request thread and cause timeouts. If the API timed out after the receiver processed the event, the caller might retry and create duplicates. With asynchronous delivery, the API commits the event first and workers handle delivery independently. The trade-off is that delivery is delayed and the system needs durable queues, retries, and status tracking.

### Q3. Why PostgreSQL, Kafka, Redis, and ClickHouse? Why not use one database?
They serve different jobs. PostgreSQL stores authoritative state and enforces transactions and constraints. Kafka transports work between independent workers and absorbs bursts. Redis speeds up frequently read values such as token and quota configuration, but cached values can be rebuilt. ClickHouse is optimized for large analytical queries. Using one database would reduce operational parts, but it would force transactional API workloads, queue traffic, cache reads, and analytics scans to compete for the same resources.

### Q4. Why use a transactional outbox instead of writing to PostgreSQL and then publishing to Kafka?
Those two operations cannot normally be committed as one atomic transaction. If PostgreSQL commits and Kafka publishing fails, the event could remain stored but never be processed. If Kafka succeeds and the database transaction rolls back, workers could receive an event that has no corresponding database record. The outbox row is written in the same PostgreSQL transaction as the event. A publisher retries unsent rows. This avoids losing the link between the database change and the message, though duplicate publication is still possible and consumers must handle it.

### Q5. Why return `202 Accepted` rather than `200 OK`?
`202 Accepted` communicates that the request has been accepted for asynchronous processing, not that every destination has received the webhook. RelayForge returns it only after the event and outbox record are committed. The response should include an event identifier so the caller can trace it. If the database transaction fails, the API must not return a successful acceptance response.

### Q6. Is RelayForge exactly-once?
No. The system is designed for at-least-once processing: a message or delivery attempt may happen more than once. A worker can complete an external HTTP call and crash before saving success, leaving the system unsure whether the receiver processed it. We use stable delivery IDs, duplicate-safe database operations, and a stable identifier in the webhook request so receivers can deduplicate. We cannot guarantee exactly-once effects inside an external system that we do not control.

### Q7. Why is there no delivery ordering guarantee in v1?
Parallel workers and retries can cause a later event to finish before an earlier event. Enforcing ordering requires identifying the ordering key, such as organization plus endpoint, and ensuring only one event for that key is processed at a time. That reduces parallelism for a busy key and requires careful retry handling. If a customer needs ordering, I would add it as an explicit mode rather than claiming it by default.

---

## 2. Scaling from a small deployment to very large traffic

### Q8. What would you change if RelayForge grew from 100 organizations to 100,000?
I would not immediately split every service or database. First I would measure event intake rate, delivery throughput, queue lag, database connection use, retry volume, and latency by organization. I would scale stateless API and worker replicas horizontally, partition work by a stable key, tune database indexes and connection pools, and set per-organization quotas. If PostgreSQL became the limiting factor after query and schema tuning, I would separate workloads or partition/shard the data. Each step should respond to measured limits, because every additional service or shard increases operational complexity.

### Q9. What if event intake suddenly jumps 20 times for five minutes?
Kafka can absorb a temporary burst, provided its storage and retention are sized for it. The API still needs rate limits and database capacity because events and outbox rows must be committed before acceptance. I would watch outbox age, Kafka consumer lag, database write latency, and disk use. Scale publishers and consumers where parallelism allows. If the database cannot commit safely, reject or throttle requests with a retryable response instead of accepting events that cannot be durably stored. A queue does not remove a database bottleneck before the queue.

### Q10. How do you estimate worker capacity?
Measure average processing time and successful completions per second under realistic endpoint latency. As a rough starting point, concurrency needed is approximately target deliveries per second multiplied by average time per delivery in seconds. For example, 2,000 deliveries/second at an average 0.2 seconds in flight implies about 400 concurrent requests, before safety margin and tail latency. Then test slow endpoints, retries, rate limits, and database writes. Do not treat this estimate as a benchmark; validate it with load tests.

### Q11. What if one organization sends 80% of all events?
That organization could consume database write capacity, Kafka partitions, and delivery workers. I would apply per-organization intake and delivery quotas, fair scheduling, and limits on concurrent deliveries. If its traffic still dominates, route its work into dedicated Kafka partitions or a separate worker pool. The trade-off is that strict isolation uses resources less efficiently when the organization is quiet. Quotas must be enforced close to admission and delivery, not only in dashboards.

### Q12. When would you partition or shard PostgreSQL?
First I would inspect slow queries, indexes, transaction duration, vacuum health, lock waits, and connection saturation. Table partitioning can help with very large time-based tables and retention, but it does not automatically distribute writes across machines. Sharding across database instances becomes reasonable when one instance cannot meet measured storage, write, or isolation requirements after tuning. A shard key such as `organization_id` keeps an organization's records together, but creates uneven-shard risk and makes cross-organization queries harder. Moving organizations between shards also needs a migration plan.

### Q13. Would you partition Kafka by organization ID?
It is a reasonable starting key because it groups one organization's messages and can support per-organization ordering within a topic partition. However, a very large organization may become a hot partition, and Kafka ordering only applies within a partition, not across all partitions or topics. If ordering is not required, a more distributed key such as event or delivery ID can spread load better. Choose the key based on the ordering contract and observed traffic, not by habit.

### Q14. What if Kafka consumer lag keeps increasing?
First determine whether the bottleneck is consumer CPU, database writes, external HTTP latency, partition count, retries, or a downstream dependency. Compare incoming and outgoing message rates and inspect lag per partition, not only total lag. Add consumers only up to the number of partitions and only if the downstream database or endpoints can handle the extra work. If external endpoints are slow, more consumers may just increase open connections and timeouts. Apply backpressure and per-endpoint concurrency limits where needed.

### Q15. What if PostgreSQL becomes unavailable?
The API cannot safely acknowledge new events because it cannot commit the event and outbox row. It should fail with a retryable server response and expose degraded health. Workers should retry database operations with bounded backoff rather than spin. Kafka messages should not be acknowledged before their required database effects are committed. Redis may fall back to PostgreSQL for some cache reads, but it cannot replace PostgreSQL's authoritative state. Recovery should include checking replication/recovery status and whether outbox and consumer lag are draining.

---

## 3. SaaS URLs, organization isolation, and deployment

### Q16. How would you give every organization its own URL, like `acme.relayforge.com`, while using the same infrastructure?
Use a wildcard DNS record such as `*.relayforge.com` pointing to the same load balancer or ingress. Configure a wildcard TLS certificate or automated certificate issuance. The gateway reads the hostname and maps the subdomain to an organization using a trusted domain mapping table. It then passes the resolved organization ID to the application. Every database query and authorization check still uses that organization ID. The hostname is a way to find the organization, not proof that the caller is allowed to access its data.

### Q17. Could a customer use its own domain, such as `webhooks.acme.com`?
Yes. Add a domain registration flow: customer proves domain ownership with a DNS TXT record or equivalent challenge; RelayForge verifies it; then the domain is mapped to the organization and its TLS certificate is issued and renewed. Reject domains already assigned elsewhere. Support certificate expiry alerts and safe removal. Do not trust a `Host` header alone; validate it against registered domains to prevent host-header attacks.

### Q18. How do you stop one organization from reading another organization's data?
Resolve the organization from authenticated credentials or a validated domain mapping, then enforce organization scope in every query and write. Do not accept a client-provided `organization_id` as authorization by itself. Add organization ID to relevant rows, unique constraints, indexes, and repository methods. Test cross-organization access explicitly for every API. PostgreSQL row-level security can provide a second enforcement layer, but it requires reliable transaction-scoped session settings and careful connection-pool handling; it does not replace application authorization.

### Q19. Would every organization need its own database?
Not by default. Shared tables with organization-scoped rows are cheaper to operate and simpler for a product with many small customers. Larger or regulated customers may need dedicated databases or separate deployments for stronger isolation, data residency, or custom maintenance windows. I would support a placement mapping that routes an organization to its assigned database, but introduce this only when a real requirement justifies the migration and operational cost.

### Q20. How would you support a large customer on dedicated infrastructure but smaller customers on shared infrastructure?
Keep a consistent API and organization identity model, then introduce a placement registry that maps each organization to a deployment or database cluster. Provisioning decides the placement. Requests resolve the organization and route to the assigned environment. Shared and dedicated deployments need the same schema version, observability, backup policy, and deployment process. The hard parts are moving a customer safely, handling domain and credential migration, and ensuring background jobs are routed to the correct placement.

### Q21. How do you deploy a new version without losing events?
Use backward-compatible database migrations: add new nullable columns or tables first, deploy code that can read old and new forms, backfill if needed, then remove old fields in a later release. Run API and worker versions that can coexist during rollout. Kafka message schemas should be versioned and old consumers should tolerate new optional fields. Drain or safely stop workers before incompatible changes. Use readiness checks, gradual rollout, and a rollback plan. A code rollback does not automatically undo a destructive database migration.

### Q22. How do you handle secrets and encryption keys in production?
Never store API keys in plaintext; show the raw key only at creation and store a one-way hash for later verification. Encrypt endpoint authentication configuration using application-level envelope encryption with a managed key service. Store key identifiers with ciphertext so data can be re-encrypted during key rotation. Restrict decryption permission to the service that needs it, avoid logging secrets, and audit key use. Database encryption at rest protects disks but does not replace application-level access control.

---

## 4. Delivery correctness, retries, and failure cases

### Q23. What if the receiver processed the webhook but RelayForge timed out?
RelayForge cannot know whether the receiver processed the request. It may retry, which can create a duplicate effect if the receiver is not idempotent. Send a stable `delivery_id` with every attempt and document that the receiver should store processed IDs and return success for duplicates. Keep the ID stable across retries of the same delivery; a deliberate replay creates a new delivery ID and generation in the current design.

### Q24. How do retries avoid overwhelming a failed endpoint?
Use exponential backoff with jitter and a maximum attempt count. For example, increase the delay after each failure, add randomness so many jobs do not retry at the same instant, and respect `Retry-After` for HTTP 429 when valid. Persist `next_attempt_at` so a restart does not reset the schedule. Limit concurrent requests per endpoint and organization. Retries should be limited to timeouts, network errors, 408, 429, and selected 5xx responses; most other 4xx responses indicate a request or configuration problem and should not be retried automatically.

### Q25. Why store retry time in PostgreSQL instead of sleeping in a worker?
Sleeping workers waste concurrency and lose in-memory schedule state on restart. Persisting `next_attempt_at` lets a scheduler claim due work after a restart and lets operators inspect when a retry should happen. The scheduler needs indexed due-work queries, bounded batch sizes, and safe claiming/leases so two workers do not own the same delivery simultaneously. At very high retry volume, a delayed-message mechanism may reduce database polling, but PostgreSQL remains the source of truth for the delivery state.

### Q26. What happens if a worker crashes after claiming a delivery?
A claim should include a lease expiry. If the worker crashes, another worker can reclaim the delivery after the lease expires. The worker must verify ownership or a lease token before writing completion so a delayed old worker cannot overwrite a newer attempt's state. The external HTTP call can still have happened before the crash, so the receiver-side idempotency key remains necessary.

### Q27. How do you prevent two workers from delivering the same job at once?
Use an atomic database claim, lease, and state transition rather than “read then update” in separate operations. A worker claims eligible rows using a transaction and a pattern such as `FOR UPDATE SKIP LOCKED`, sets lease metadata, and commits. Other workers skip locked or already leased rows. Completion checks the current lease/ownership token. This prevents most concurrent claims, but it cannot make an external HTTP call and a database update one atomic operation.

### Q28. How do you handle HTTP 429 and per-endpoint rate limits?
Treat 429 as a signal to slow down, inspect `Retry-After`, and schedule the next attempt accordingly. Maintain per-endpoint concurrency or rate limits so one receiver is not flooded. A Redis limiter can coordinate across workers, but define what happens when Redis is unavailable; a bounded local limiter or conservative fail-closed behavior may be appropriate depending on the contract. The limiter must account for all worker replicas, not only each process independently.

### Q29. How do you prevent SSRF when customers register webhook URLs?
Validate the URL at registration and again immediately before connecting, because DNS can change. Allow only supported schemes, normally HTTPS; reject loopback, private, link-local, metadata-service, and other forbidden IP ranges for both IPv4 and IPv6. Resolve DNS safely, validate all resolved addresses, disable redirects or revalidate every redirect target, and defend against DNS rebinding. Network-level egress restrictions provide another layer. URL validation alone is not enough if the HTTP client later connects to a different resolved address.

### Q30. What if an endpoint's authentication token service is down?
Do not send a webhook with an invalid or missing token. Attempt token acquisition using the configured bounded retry window; fail fast for permanent configuration errors such as an invalid client configuration. Cache valid tokens using a key that includes organization, endpoint, auth configuration version, and credential version. Use a single-flight mechanism and distributed coordination to avoid every worker refreshing the same token at once. Do not log token responses or credentials.

### Q31. How would you add delivery ordering later?
First define the exact guarantee, for example ordering per endpoint rather than global ordering. Assign a sequence or ordered queue key per endpoint and allow only the earliest unfinished delivery for that key to proceed. A failed earlier delivery creates a policy choice: block later deliveries until it succeeds, or skip it after a defined terminal state. The first option preserves order but can block a busy endpoint; the second improves throughput but weakens the guarantee. Partitioning Kafka by endpoint ID can help, but retries and database scheduling must preserve the same ordering rule too.

### Q32. What does dead-lettering mean here, and how does replay work?
After the configured retry limit or a terminal failure, mark the delivery as dead-lettered and retain its attempts and error details. An operator can inspect the cause and request replay. Replay creates a new delivery ID and increments the replay generation, while preserving the original history. It must re-check endpoint status, authorization, quotas, and current security policy. Restrict replay permissions and audit who initiated it, because replay can repeat external side effects.

---

## 5. Database and analytics choices

### Q33. Why keep delivery state in PostgreSQL and analytics in ClickHouse?
Operational APIs need current state, transactional updates, and constraints, which fit PostgreSQL. Analytics queries may scan millions of facts over time ranges and group by organization, endpoint, status, or day, which is better suited to ClickHouse. Analytics is eventually consistent; an event may appear there after a delay. If a customer asks for the current delivery state, read PostgreSQL rather than treating ClickHouse as authoritative.

### Q34. What if ClickHouse is unavailable?
Delivery processing should continue because ClickHouse is not the source of truth for delivery state. Analytics facts should remain recoverable through the analytics outbox/Kafka path and its retention policy. Consumers should retry and avoid committing Kafka offsets until the insert has succeeded. Since duplicate inserts can occur, the ClickHouse table uses a stable fact ID and version with `ReplacingMergeTree`; background deduplication is eventual. Queries that require accurate results must deduplicate at query time, for example with `argMax` or `FINAL` where appropriate.

### Q35. Why not immediately create daily aggregate tables in ClickHouse?
An incremental aggregate can count the same Kafka event twice before duplicate rows are merged. I would first make raw fact ingestion reliable and define deduplication clearly. Then add rollups using a process that is safe under replay and duplicates, such as deduplicated source batches or rebuildable aggregates. Rollups improve query speed but add correction, backfill, and reconciliation work.

### Q36. How would you implement 30-day data retention?
Define retention separately for event payloads, delivery attempts, outbox rows, and analytics facts. A scheduled purge deletes only eligible records in bounded batches, using indexes and partition drops where suitable. Do not delete unpublished outbox rows. Preserve records required for audit or replay according to the product's policy. Test that cleanup does not remove rows still referenced by active work and monitor deletion lag. The current design documents a 30-day retention target, but automated purge must not be claimed until implemented and tested.

### Q37. How do you stop analytics queries from harming delivery APIs?
Keep analytics reads off PostgreSQL's primary operational workload where possible. Use ClickHouse for time-range reports, require organization scope and bounded date ranges, apply query timeouts and result limits, and rate-limit expensive reports. If analytics queries run on the same service, isolate their connection pools and concurrency. Track query duration and memory use. An analytics endpoint should expose an “as of” timestamp because the data is eventually consistent.

---

## 6. API design, idempotency, and product behavior

### Q38. How should publish API idempotency work?
Accept an idempotency key scoped to the authenticated organization and store it with a unique constraint, along with a hash of the request. If the same key and same request arrive again, return the original event result rather than creating a second event. If the key is reused with a different request body, return a conflict. Define how long keys are retained and what happens after expiry. The database uniqueness constraint is the final protection against concurrent duplicate requests.

### Q39. Why does publish use `event_type` rather than an endpoint ID?
The producer describes what happened, not where it should be delivered. RelayForge routes the event to all active subscriptions for that organization and event type. This keeps producers independent of the customer's webhook configuration. The trade-off is that one publish can create many delivery jobs, so routing must handle fan-out, quota checks, and large subscription counts safely.

### Q40. What if an event type has 50,000 subscribed endpoints?
Do not create all delivery rows in one long API transaction. The publish API should store the event and outbox record, then asynchronous routing processes subscriptions in bounded batches. Use indexes on organization and event type, track routing progress, and make delivery creation idempotent with a unique key. Apply organization quotas and backpressure. Very large fan-out may need a fan-out job or cursor-based batch process so a worker restart can continue without starting from zero.

### Q41. How would you version the public API?
Keep compatible changes additive: optional request fields, optional response fields, and new endpoints. Use a major version for breaking changes and document deprecation dates. Validate input at the boundary and maintain contract tests for clients. Internal Kafka event schemas should be versioned separately from the public HTTP API, because the two evolve for different reasons.

### Q42. How do you make webhook signatures safe and verifiable?
Sign a canonical representation of the request body plus a timestamp using HMAC with a secret shared with the receiver. Include the signature version and timestamp in headers. The receiver recomputes the signature and compares it using a constant-time comparison, and rejects timestamps outside a reasonable window to limit replay. Define the exact bytes being signed and how secrets rotate; otherwise harmless serialization differences can break verification.

---

## 7. Operations, testing, and measurable performance

### Q43. What metrics would you monitor in production?
Track API request rate, error rate, and latency; accepted events per second; outbox oldest-row age; Kafka consumer lag; delivery success rate and latency; retry and dead-letter counts; due deliveries waiting; lease expirations; PostgreSQL connection use, locks, slow queries, and disk; Redis hit rate and errors; ClickHouse insert failures and analytics lag. Break down by organization and endpoint where safe, but avoid unbounded metric labels that create huge monitoring costs.

### Q44. What would you test before calling the project production-ready?
Test API validation and authorization, cross-organization access, idempotent publish, endpoint subscription transactions, outbox recovery, duplicate Kafka messages, worker crashes, lease expiry, retry timing, 429 handling, token refresh races, SSRF restrictions, replay permissions, ClickHouse outages, and retention cleanup. Run end-to-end tests in Docker with a controllable webhook receiver. Add load tests for both event intake and delivery, because they stress different parts of the system.

### Q45. How would you prove the “50K load” claim?
Define exactly what 50K means: 50,000 events total, 50,000 events per minute, or 50,000 events per second are very different targets. Record payload size, endpoint count, delivery fan-out, duration, success criteria, machine resources, replica counts, and whether the test includes retries. Measure throughput, p50/p95/p99 latency, error rate, queue lag, database CPU/connections, and delivery completion time. Publish the reproducible test command and results. Do not put a target on the resume as achieved until the benchmark has actually passed.

### Q46. What is the first bottleneck you expect?
It depends on workload. Event intake can be limited by PostgreSQL writes and outbox polling. Delivery can be limited by external endpoint latency, connection pools, or per-endpoint limits. Analytics can be limited by ClickHouse insert/query load. I would identify the bottleneck from measurements rather than assume Kafka is the first limit. Scaling the wrong component adds cost without increasing end-to-end throughput.

### Q47. How would you recover after a region-wide outage?
Keep durable database backups and a tested restore process. For a high-availability target, define a recovery point objective (how much recent data may be lost) and recovery time objective (how long recovery may take). Replicate data and infrastructure to another region if the target justifies the cost, then test failover and DNS/routing changes. Decide how to handle in-flight deliveries and duplicate attempts after recovery. Multi-region active-active writes are substantially more complex and should not be introduced without a clear need.

### Q48. How would you reduce cost without weakening correctness?
Set retention and payload limits, batch outbox publishing and ClickHouse inserts, scale workers from queue depth and oldest-job age, and use different worker pools for fast and slow destinations. Avoid over-provisioning every service for peak traffic if queues can safely absorb bursts. Keep required durable state in PostgreSQL and do not remove retry or audit data merely to save storage without a documented policy. Measure cost per accepted event and per completed delivery.

---

## 8. Design alternatives and trade-offs

### Q49. If starting again, what might you simplify?
I would keep the core reliability path: PostgreSQL transaction plus outbox, asynchronous delivery, persisted retry state, and receiver-visible delivery IDs. I would initially keep the number of deployable services small if one team operates the project, while maintaining clear module boundaries. I would defer dedicated customer databases, multi-region active-active, custom domains, complex rollups, and ordering guarantees until requirements or measurements justify them. The aim is to preserve correctness without building operational machinery too early.

### Q50. Why not use Redis as the queue?
Redis can support queues, but RelayForge needs durable event state, retry scheduling, audit history, and recovery that are consistent with PostgreSQL. Using Redis as the only queue can create a second source of truth unless carefully designed. Kafka provides retained messages and consumer groups for asynchronous transport; PostgreSQL records authoritative delivery state. Redis remains useful for caching and distributed rate coordination, not as the authoritative record of whether an event or delivery exists.

### Q51. Why not use a managed queue instead of Kafka?
A managed queue can be simpler to operate and may be enough for this product. Kafka is useful when multiple independent consumer groups need the same event stream, replay from retained offsets, and high sustained throughput. The choice depends on operational skill, message retention needs, ordering, cost, and throughput. I would compare the managed queue's delivery, visibility timeout, redrive, ordering, and retention semantics against the actual RelayForge requirements rather than choose Kafka only because it is common in distributed systems.

### Q52. Why not use microservices for every component?
Separate services help when components need different scaling, release cycles, or failure boundaries. They also add network calls, deployments, configuration, tracing, and failure handling. A modular application with separate worker processes may be sufficient initially. I would split components when independent scaling or ownership gives a measurable benefit, while keeping database transactions and contracts explicit.

### Q53. Would you support customer-specific retry policies?
Potentially, but expose a constrained policy rather than arbitrary code. Let customers configure safe bounds for retry count, delay limits, and perhaps endpoint concurrency. Validate policy values and store versioned configuration. Enforce platform-wide maximums so one configuration cannot cause retry storms or consume all capacity. Policy changes should affect future scheduling in a defined way and be auditable.

### Q54. How would you handle data residency or regulated customers?
First identify the exact legal and contractual requirement: where data is stored, where it is processed, retention, auditability, and who can access secrets. Then place the organization in an approved region or dedicated environment and ensure logs, backups, analytics, and support access follow the same policy. A regional database alone is insufficient if payloads or logs are copied elsewhere. This requirement can justify separate deployments, but increases cost and operational work.

---

## 9. Useful follow-up prompts to practise aloud

For each prompt, answer in 60–90 seconds and draw the relevant flow or table when useful.

- “Your API returns 202, but the webhook never arrives. How do you debug it?”
- “Kafka delivered the same message three times. What prevents duplicate delivery rows?”
- “The receiver is slow and has started returning 429. What changes first?”
- “One organization sends 10 times more traffic than everyone else. How do you protect other customers?”
- “A customer asks for strict ordering. What exact guarantee can you offer?”
- “An attacker registers a URL pointing to cloud metadata. How do you block it?”
- “The ClickHouse dashboard is 10 minutes behind. Does that affect delivery correctness?”
- “The database is full because outbox rows are accumulating. What do you do?”
- “You need to move one organization to a dedicated database without downtime. What is your migration plan?”
- “What would you measure before adding another Kafka partition or PostgreSQL shard?”
- “Which part of RelayForge is currently implemented, and which parts are only designed?”
- “What was the hardest trade-off, and what evidence would make you change the design?”

## Final interview discipline

- Be exact about the guarantee: accepted, queued, attempted, delivered, or confirmed by the receiver are different states.
- State the failure window. For example, an HTTP timeout does not prove that the receiver did not process the request.
- Explain the data that makes recovery possible: event ID, delivery ID, attempt history, outbox state, lease expiry, and `next_attempt_at`.
- State the trade-off after the solution. Ordering reduces parallelism; dedicated databases cost more; more retries can increase load; caching introduces staleness.
- Do not claim exactly-once delivery, zero downtime, infinite scaling, or a benchmark without evidence.

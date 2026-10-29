# A3 organization provisioning and API keys

The control plane owns organization, user, key, and audit writes. Each provisioning operation uses one PostgreSQL transaction. API requests use an organization ID derived from a validated key; a requested organization ID never grants access by itself.

## Local policy

- The initial supported plan is `FREE`. Its default quotas are 600 events per minute, 25 active endpoints, and 100 in-flight deliveries, matching the LLD example. Overrides must be positive integers no greater than provisional platform bounds of 60,000, 1,000, and 10,000 respectively. These bounds are authorization policy, not measured capacity.
- Initial keys have `events:write` and `organizations:read` scopes. Rotation can replace the scope set with an allowlisted set. Key creation and rotation return the plaintext once; the database stores a 32-byte HMAC-SHA-256 digest of a 256-bit random secret. The public key ID is 128 random bits. The HMAC key is generated into the ignored `.env` file for local use and must remain stable while issued keys are active.
- A separate trusted provisioning credential protects organization and user creation, key rotation and revocation, and recovery lookup. It is read from `.env` locally. No normal organization key can call these operations.
- Organization creation requires a 16–100 character `Idempotency-Key`. The control plane stores only its SHA-256 digest in `provisioning_requests`, uniquely. Repeating a committed request returns `409` without a key. A trusted `GET /v1/organizations/recover` with the same header returns the organization ID and public key ID, allowing an operator to rotate the key if the first response was lost. The key secret is never stored for replay.
- `GET /v1/organizations/{id}` applies the authenticated organization ID in its SQL predicate and returns `404` for another organization's ID. The control-plane authentication filter defaults to denying new `/v1` routes until their required scope is declared.

The implementation follows [Spring JDBC's shared template and transaction guidance](https://docs.spring.io/spring-framework/reference/data-access/jdbc/core.html), [Spring's transaction-bound connection behavior](https://docs.spring.io/spring-framework/reference/data-access/jdbc/connections.html), and the [OWASP identifier/verifier storage guidance](https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html). The exact local plan limits and route choices above are RelayForge decisions, not values inferred from those sources.

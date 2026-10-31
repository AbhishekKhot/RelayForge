package com.relayforge.controlplane.endpoint;

import com.relayforge.controlplane.endpoint.auth.EndpointSecretCipher.EncryptedSecret;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public final class JdbcEndpointRepository implements EndpointRepository {
    private static final RowMapper<EndpointView> ENDPOINT_VIEW = (row, index) -> new EndpointView(
            row.getObject("id", UUID.class), row.getString("name"), row.getString("status"),
            row.getString("auth_type"), row.getTimestamp("created_at").toInstant(), row.getLong("version"));
    private static final RowMapper<SubscriptionView> SUBSCRIPTION_VIEW = (row, index) ->
            new SubscriptionView(row.getObject("id", UUID.class), row.getObject("endpoint_id", UUID.class),
                    row.getString("event_type"), row.getString("status"), row.getLong("version"));
    private final JdbcTemplate jdbc;

    public JdbcEndpointRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void insert(UUID id, UUID organizationId, String name, String baseUrl, String path,
            String queryJson, String headersJson, String metadataJson,
            int timeoutMs, int maxConcurrency, int maxAttempts, Instant createdAt,
            String authType, EncryptedSecret authConfig) {
        jdbc.update("""
                INSERT INTO endpoints (id, organization_id, name, base_url, path, query_params, headers,
                  metadata, auth_type, auth_config_ciphertext, auth_wrapped_dek, auth_key_version,
                  auth_nonce, auth_config_version, timeout_ms, max_concurrency, max_attempts, created_at)
                VALUES (?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?::jsonb, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, id, organizationId, name, baseUrl, path, queryJson, headersJson, metadataJson,
                authType, authConfig == null ? null : authConfig.ciphertext(),
                authConfig == null ? null : authConfig.wrappedDek(),
                authConfig == null ? null : authConfig.keyVersion(),
                authConfig == null ? null : authConfig.nonce(),
                authConfig == null ? null : 1L,
                timeoutMs, maxConcurrency, maxAttempts, Timestamp.from(createdAt));
    }

    @Override
    public Optional<EndpointView> findForOrganization(UUID organizationId, UUID endpointId) {
        return jdbc.query("""
                SELECT id, name, status, auth_type, created_at, version
                FROM endpoints WHERE organization_id = ? AND id = ?
                """, ENDPOINT_VIEW, organizationId, endpointId)
                .stream().findFirst();
    }

    @Override
    public Optional<AuthenticationState> findAuthentication(UUID organizationId, UUID endpointId) {
        return jdbc.query("""
                SELECT auth_type, headers::text AS headers_json, auth_config_ciphertext,
                  auth_wrapped_dek, auth_key_version, auth_nonce, auth_config_version
                FROM endpoints WHERE organization_id = ? AND id = ?
                """, (row, index) -> new AuthenticationState(row.getString("auth_type"),
                row.getString("headers_json"), row.getBytes("auth_config_ciphertext") == null ? null
                        : new EncryptedSecret(row.getBytes("auth_config_ciphertext"),
                                row.getBytes("auth_wrapped_dek"), row.getString("auth_key_version"),
                                row.getBytes("auth_nonce")),
                row.getObject("auth_config_version", Long.class)), organizationId, endpointId)
                .stream().findFirst();
    }

    @Override
    public List<EndpointView> listForOrganization(UUID organizationId, UUID afterId, int limit) {
        return jdbc.query("""
                SELECT id, name, status, auth_type, created_at, version
                FROM endpoints WHERE organization_id = ? AND (?::uuid IS NULL OR id > ?::uuid)
                ORDER BY id ASC LIMIT ?
                """, ENDPOINT_VIEW, organizationId, afterId, afterId, limit);
    }

    @Override
    public List<SubscriptionView> listSubscriptions(UUID organizationId, UUID afterId, int limit) {
        return jdbc.query("""
                SELECT id, endpoint_id, event_type, status, version
                FROM subscriptions WHERE organization_id = ? AND (?::uuid IS NULL OR id > ?::uuid)
                ORDER BY id ASC LIMIT ?
                """, SUBSCRIPTION_VIEW, organizationId, afterId, afterId, limit);
    }

    @Override
    public List<SubscriptionView> subscriptionsForEndpoint(UUID organizationId, UUID endpointId) {
        return jdbc.query("""
                SELECT id, endpoint_id, event_type, status, version
                FROM subscriptions WHERE organization_id = ? AND endpoint_id = ? ORDER BY id ASC
                """, SUBSCRIPTION_VIEW, organizationId, endpointId);
    }

    @Override
    public Optional<Long> update(UUID organizationId, UUID endpointId, long expectedVersion,
            EndpointUpdate update) {
        EncryptedSecret auth = update.authConfig();
        return jdbc.query("""
                UPDATE endpoints SET name = COALESCE(?, name), status = COALESCE(?, status),
                  base_url = COALESCE(?, base_url), path = COALESCE(?, path),
                  query_params = COALESCE(?::jsonb, query_params),
                  headers = COALESCE(?::jsonb, headers), metadata = COALESCE(?::jsonb, metadata),
                  timeout_ms = COALESCE(?, timeout_ms), max_concurrency = COALESCE(?, max_concurrency),
                  max_attempts = COALESCE(?, max_attempts),
                  auth_type = CASE WHEN ? THEN ? ELSE auth_type END,
                  auth_config_ciphertext = CASE WHEN ? THEN ? ELSE auth_config_ciphertext END,
                  auth_wrapped_dek = CASE WHEN ? THEN ? ELSE auth_wrapped_dek END,
                  auth_key_version = CASE WHEN ? THEN ? ELSE auth_key_version END,
                  auth_nonce = CASE WHEN ? THEN ? ELSE auth_nonce END,
                  auth_config_version = CASE WHEN ? THEN ? ELSE auth_config_version END,
                  updated_at = clock_timestamp(), version = version + 1
                WHERE organization_id = ? AND id = ? AND version = ?
                RETURNING version
                """, (row, index) -> row.getLong(1),
                update.name(), update.status(), update.baseUrl(), update.path(), update.queryJson(),
                update.headersJson(), update.metadataJson(), update.timeoutMs(), update.maxConcurrency(),
                update.maxAttempts(), update.replaceAuthentication(), update.authType(),
                update.replaceAuthentication(), auth == null ? null : auth.ciphertext(),
                update.replaceAuthentication(), auth == null ? null : auth.wrappedDek(),
                update.replaceAuthentication(), auth == null ? null : auth.keyVersion(),
                update.replaceAuthentication(), auth == null ? null : auth.nonce(),
                update.replaceAuthentication(), auth == null ? null : expectedVersion + 1,
                organizationId, endpointId, expectedVersion).stream().findFirst();
    }

    @Override
    public Optional<Long> rotateSigningSecret(UUID organizationId, UUID endpointId, long expectedVersion,
            EncryptedSecret secret) {
        return jdbc.query("""
                UPDATE endpoints SET signing_secret_ciphertext = ?, signing_wrapped_dek = ?,
                  signing_key_version = ?, signing_nonce = ?, signing_secret_version = ?,
                  updated_at = clock_timestamp(),
                  version = version + 1
                WHERE organization_id = ? AND id = ? AND version = ?
                RETURNING version
                """, (row, index) -> row.getLong(1), secret.ciphertext(), secret.wrappedDek(),
                secret.keyVersion(), secret.nonce(), expectedVersion + 1,
                organizationId, endpointId, expectedVersion)
                .stream().findFirst();
    }

    @Override
    public long nextConfigurationRevision(UUID organizationId) {
        Long revision = jdbc.queryForObject("""
                UPDATE organizations SET config_revision = config_revision + 1,
                  updated_at = clock_timestamp() WHERE id = ? RETURNING config_revision
                """, Long.class, organizationId);
        if (revision == null) {
            throw new IllegalStateException("Configuration revision was not returned");
        }
        return revision;
    }

    @Override
    public void insertConfigurationChange(UUID organizationId, long revision, UUID endpointId,
            String payloadJson) {
        jdbc.update("""
                INSERT INTO control_outbox (organization_id, source_revision, aggregate_type, aggregate_id,
                  message_type, topic, partition_key, payload)
                VALUES (?, ?, 'endpoint', ?, 'configuration.changed.v1',
                  'relayforge.config.changed.v1', ?, ?::jsonb)
                """, organizationId, revision, endpointId.toString(), organizationId.toString(), payloadJson);
    }

    @Override
    public void insertSubscription(UUID id, UUID organizationId, UUID endpointId, String eventType) {
        jdbc.update("""
                INSERT INTO subscriptions (id, organization_id, endpoint_id, event_type)
                VALUES (?, ?, ?, ?)
                """, id, organizationId, endpointId, eventType);
    }

    @Override
    public void insertAudit(UUID organizationId, UUID endpointId, String action, String requestId) {
        jdbc.update("""
                INSERT INTO audit_records (organization_id, action, resource_type, resource_id, request_id)
                VALUES (?, ?, 'endpoint', ?, ?)
                """, organizationId, action, endpointId, requestId);
    }
}

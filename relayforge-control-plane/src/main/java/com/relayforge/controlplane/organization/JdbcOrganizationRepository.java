package com.relayforge.controlplane.organization;

import com.relayforge.controlplane.security.ApiKeyCodec;
import java.sql.Array;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;

public final class JdbcOrganizationRepository implements OrganizationRepository {
    private final JdbcTemplate jdbc;

    public JdbcOrganizationRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void insertOrganization(UUID id, String name, String planCode, String quotaJson) {
        jdbc.update("INSERT INTO organizations (id, name, plan_code, quota_config) VALUES (?, ?, ?, ?::jsonb)",
                id, name, planCode, quotaJson);
    }

    @Override
    public void insertProvisioningRequest(byte[] requestHash, UUID organizationId, String keyId) {
        jdbc.update("INSERT INTO provisioning_requests (request_hash, organization_id, key_id) VALUES (?, ?, ?)",
                requestHash, organizationId, keyId);
    }

    @Override
    public Optional<ProvisioningRequest> findProvisioningRequest(byte[] requestHash) {
        return jdbc.query("SELECT organization_id, key_id FROM provisioning_requests WHERE request_hash = ?",
                (result, rowNumber) -> new ProvisioningRequest(
                        result.getObject("organization_id", UUID.class), result.getString("key_id")),
                requestHash).stream().findFirst();
    }

    @Override
    public void insertKey(UUID id, UUID organizationId, ApiKeyCodec.IssuedKey key, List<String> scopes) {
        jdbc.update("INSERT INTO api_keys (id, organization_id, key_id, secret_hash, scopes) "
                        + "VALUES (?, ?, ?, ?, ?::text[])",
                id, organizationId, key.keyId(), key.digest(), "{" + String.join(",", scopes) + "}");
    }

    @Override
    public Optional<StoredKey> findKey(String keyId) {
        List<StoredKey> rows = jdbc.query("SELECT k.id, k.organization_id, k.key_id, k.secret_hash, k.scopes, "
                        + "k.expires_at, k.revoked_at, o.status AS organization_status "
                        + "FROM api_keys k JOIN organizations o ON o.id = k.organization_id WHERE k.key_id = ?",
                (result, rowNumber) -> new StoredKey(
                        result.getObject("id", UUID.class),
                        result.getObject("organization_id", UUID.class),
                        result.getString("key_id"),
                        result.getBytes("secret_hash"),
                        readScopes(result.getArray("scopes")),
                        result.getString("organization_status"),
                        result.getTimestamp("expires_at") == null ? null
                                : result.getTimestamp("expires_at").toInstant(),
                        result.getTimestamp("revoked_at") == null ? null
                                : result.getTimestamp("revoked_at").toInstant()),
                keyId);
        return rows.stream().findFirst();
    }

    @Override
    public boolean touchActiveKey(UUID id, String requiredScope) {
        return jdbc.update("UPDATE api_keys k SET last_used_at = clock_timestamp() FROM organizations o "
                        + "WHERE k.id = ? AND o.id = k.organization_id AND o.status = 'ACTIVE' "
                        + "AND k.revoked_at IS NULL AND (k.expires_at IS NULL OR k.expires_at > clock_timestamp()) "
                        + "AND ? = ANY(k.scopes)",
                id, requiredScope) == 1;
    }

    @Override
    public boolean activeOrganizationExists(UUID organizationId) {
        Long count = jdbc.queryForObject("SELECT count(*) FROM organizations WHERE id = ? AND status = 'ACTIVE'",
                Long.class, organizationId);
        return count != null && count == 1;
    }

    @Override
    public Optional<Integer> findActiveEventRateLimit(UUID organizationId) {
        List<Integer> limits = jdbc.query("SELECT (quota_config->>'events_per_minute')::integer "
                        + "FROM organizations WHERE id = ? AND status = 'ACTIVE'",
                (result, rowNumber) -> result.getObject(1, Integer.class), organizationId);
        if (limits.size() != 1 || limits.get(0) == null || limits.get(0) < 1) {
            return Optional.empty();
        }
        return Optional.of(limits.get(0));
    }

    @Override
    public Optional<OrganizationView> findOrganizationForOwner(UUID requestedId, UUID authenticatedId) {
        return jdbc.query("SELECT id, name, plan_code FROM organizations WHERE id = ? AND id = ?",
                (result, rowNumber) -> new OrganizationView(
                        result.getObject("id", UUID.class), result.getString("name"), result.getString("plan_code")),
                requestedId, authenticatedId).stream().findFirst();
    }

    @Override
    public void insertUser(UUID id, UUID organizationId, String email, String name, String metadataJson) {
        jdbc.update("INSERT INTO users (id, organization_id, email, display_name, metadata) "
                        + "VALUES (?, ?, ?, ?, ?::jsonb)",
                id, organizationId, email, name, metadataJson);
    }

    @Override
    public boolean revokeKey(UUID id) {
        return jdbc.update("UPDATE api_keys SET revoked_at = clock_timestamp() WHERE id = ? AND revoked_at IS NULL",
                id) == 1;
    }

    @Override
    public void insertAudit(UUID organizationId, String action, String resourceType, UUID resourceId,
            String requestId, String detailsJson) {
        jdbc.update("INSERT INTO audit_records (organization_id, action, resource_type, resource_id, request_id, details) "
                        + "VALUES (?, ?, ?, ?, ?, ?::jsonb)",
                organizationId, action, resourceType, resourceId, requestId, detailsJson);
    }

    private List<String> readScopes(Array array) throws SQLException {
        try {
            return List.copyOf(Arrays.asList((String[]) array.getArray()));
        } finally {
            array.free();
        }
    }
}

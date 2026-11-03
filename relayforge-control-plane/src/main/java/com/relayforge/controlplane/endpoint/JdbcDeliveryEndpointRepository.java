package com.relayforge.controlplane.endpoint;

import com.relayforge.controlplane.endpoint.auth.EndpointSecretCipher.EncryptedSecret;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;

public final class JdbcDeliveryEndpointRepository implements DeliveryEndpointRepository {
    private final JdbcTemplate jdbc;

    public JdbcDeliveryEndpointRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public Optional<StoredEndpoint> find(UUID organizationId, UUID endpointId) {
        return jdbc.query("""
                SELECT endpoint.status, endpoint.version, endpoint.base_url, endpoint.path,
                    endpoint.query_params::text, endpoint.headers::text,
                    endpoint.timeout_ms, endpoint.max_attempts, endpoint.max_concurrency,
                    coalesce((organization.quota_config->>'max_in_flight_deliveries')::integer,
                        CASE organization.plan_code WHEN 'STANDARD' THEN 1000 ELSE 100 END),
                    endpoint.auth_type,
                    endpoint.auth_config_ciphertext, endpoint.auth_wrapped_dek, endpoint.auth_key_version,
                    endpoint.auth_nonce, endpoint.auth_config_version, endpoint.auth_credential_version,
                    endpoint.signing_secret_ciphertext, endpoint.signing_wrapped_dek,
                    endpoint.signing_key_version, endpoint.signing_nonce,
                    endpoint.signing_secret_version
                FROM endpoints endpoint JOIN organizations organization
                    ON organization.id = endpoint.organization_id
                WHERE endpoint.organization_id = ? AND endpoint.id = ?
                """, (row, index) -> new StoredEndpoint(row.getString(1), row.getLong(2),
                row.getString(3), row.getString(4), row.getString(5), row.getString(6),
                row.getInt(7), row.getInt(8), row.getInt(9), row.getInt(10), row.getString(11),
                row.getBytes(12) == null ? null : new EncryptedSecret(row.getBytes(12),
                        row.getBytes(13), row.getString(14), row.getBytes(15)),
                row.getObject(16, Long.class),
                row.getLong(17),
                row.getBytes(18) == null ? null : new EncryptedSecret(row.getBytes(18),
                        row.getBytes(19), row.getString(20), row.getBytes(21)),
                row.getObject(22, Long.class)), organizationId, endpointId)
                .stream().findFirst();
    }
}

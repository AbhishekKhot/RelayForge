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
                SELECT status, version, base_url, path, query_params::text, headers::text,
                    timeout_ms, max_attempts, max_concurrency, auth_type,
                    auth_config_ciphertext, auth_wrapped_dek, auth_key_version,
                    auth_nonce, auth_config_version, auth_credential_version, signing_secret_ciphertext,
                    signing_wrapped_dek, signing_key_version, signing_nonce,
                    signing_secret_version
                FROM endpoints WHERE organization_id = ? AND id = ?
                """, (row, index) -> new StoredEndpoint(row.getString(1), row.getLong(2),
                row.getString(3), row.getString(4), row.getString(5), row.getString(6),
                row.getInt(7), row.getInt(8), row.getInt(9), row.getString(10),
                row.getBytes(11) == null ? null : new EncryptedSecret(row.getBytes(11),
                        row.getBytes(12), row.getString(13), row.getBytes(14)),
                row.getObject(15, Long.class),
                row.getLong(16),
                row.getBytes(17) == null ? null : new EncryptedSecret(row.getBytes(17),
                        row.getBytes(18), row.getString(19), row.getBytes(20)),
                row.getObject(21, Long.class)), organizationId, endpointId)
                .stream().findFirst();
    }
}

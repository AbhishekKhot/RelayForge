package com.relayforge.controlplane.endpoint;

import com.relayforge.controlplane.endpoint.auth.EndpointSecretCipher.EncryptedSecret;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public interface EndpointRepository {
    void insert(UUID id, UUID organizationId, String name, String baseUrl, String path,
            String queryJson, String headersJson, String metadataJson,
            int timeoutMs, int maxConcurrency, int maxAttempts, Instant createdAt,
            String authType, EncryptedSecret authConfig);

    Optional<EndpointView> findForOrganization(UUID organizationId, UUID endpointId);

    void insertSubscription(UUID id, UUID organizationId, UUID endpointId, String eventType);

    void insertAudit(UUID organizationId, UUID endpointId, String requestId);

    record EndpointView(UUID id, String name, String status, String authType, Instant createdAt) {
    }
}

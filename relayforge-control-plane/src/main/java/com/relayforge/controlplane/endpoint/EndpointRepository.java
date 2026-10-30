package com.relayforge.controlplane.endpoint;

import java.time.Instant;
import java.util.UUID;

public interface EndpointRepository {
    void insert(UUID id, UUID organizationId, String name, String baseUrl, String path,
            String queryJson, String headersJson, String metadataJson,
            int timeoutMs, int maxConcurrency, int maxAttempts, Instant createdAt);

    void insertSubscription(UUID id, UUID organizationId, UUID endpointId, String eventType);

    void insertAudit(UUID organizationId, UUID endpointId, String requestId);
}

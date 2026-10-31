package com.relayforge.controlplane.endpoint;

import com.relayforge.controlplane.endpoint.auth.EndpointSecretCipher.EncryptedSecret;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface EndpointRepository {
    void insert(UUID id, UUID organizationId, String name, String baseUrl, String path,
            String queryJson, String headersJson, String metadataJson,
            int timeoutMs, int maxConcurrency, int maxAttempts, Instant createdAt,
            String authType, EncryptedSecret authConfig);

    Optional<EndpointView> findForOrganization(UUID organizationId, UUID endpointId);

    Optional<AuthenticationState> findAuthentication(UUID organizationId, UUID endpointId);

    List<EndpointView> listForOrganization(UUID organizationId, UUID afterId, int limit);

    List<SubscriptionView> listSubscriptions(UUID organizationId, UUID afterId, int limit);

    List<SubscriptionView> subscriptionsForEndpoint(UUID organizationId, UUID endpointId);

    Optional<Long> update(UUID organizationId, UUID endpointId, long expectedVersion, EndpointUpdate update);

    Optional<Long> rotateSigningSecret(UUID organizationId, UUID endpointId, long expectedVersion,
            EncryptedSecret secret);

    long nextConfigurationRevision(UUID organizationId);

    void insertConfigurationChange(UUID organizationId, long revision, UUID endpointId,
            String payloadJson);

    void insertSubscription(UUID id, UUID organizationId, UUID endpointId, String eventType);

    void insertAudit(UUID organizationId, UUID endpointId, String action, String requestId);

    record EndpointView(UUID id, String name, String status, String authType, Instant createdAt,
            long version) {
    }

    record SubscriptionView(UUID id, UUID endpointId, String eventType, String status, long version) {
    }

    record EndpointUpdate(String name, String status, String baseUrl, String path, String queryJson,
            String headersJson, String metadataJson, Integer timeoutMs, Integer maxConcurrency,
            Integer maxAttempts, boolean replaceAuthentication, String authType, EncryptedSecret authConfig) {
    }

    record AuthenticationState(String type, String headersJson, EncryptedSecret secret, Long version) {
        @Override
        public String toString() {
            return "AuthenticationState[type=" + type + ", headers=[REDACTED], secret=[REDACTED], "
                    + "version=" + version + "]";
        }
    }
}

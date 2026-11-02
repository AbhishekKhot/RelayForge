package com.relayforge.controlplane.endpoint;

import com.relayforge.controlplane.endpoint.auth.EndpointSecretCipher.EncryptedSecret;
import java.util.Optional;
import java.util.UUID;

public interface DeliveryEndpointRepository {
    Optional<StoredEndpoint> find(UUID organizationId, UUID endpointId);

    record StoredEndpoint(String status, long version, String baseUrl, String path,
            String queryJson, String headersJson, int timeoutMs, int maxAttempts,
            int maxConcurrency, String authType, EncryptedSecret authSecret, Long authVersion,
            EncryptedSecret signingSecret, Long signingVersion) {
        @Override
        public String toString() {
            return "StoredEndpoint[status=" + status + ", version=" + version
                    + ", destination=[REDACTED], authentication=[REDACTED], signingSecret=[REDACTED]]";
        }
    }
}

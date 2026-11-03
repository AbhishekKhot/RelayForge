package com.relayforge.controlplane.endpoint;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.controlplane.endpoint.auth.EndpointSecretCipher;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public final class DeliveryEndpointService {
    private final DeliveryEndpointRepository repository;
    private final EndpointSecretCipher cipher;
    private final ObjectMapper json;

    public DeliveryEndpointService(DeliveryEndpointRepository repository,
            EndpointSecretCipher cipher, ObjectMapper json) {
        this.repository = Objects.requireNonNull(repository);
        this.cipher = Objects.requireNonNull(cipher);
        this.json = Objects.requireNonNull(json);
    }

    public Configuration find(UUID organizationId, UUID endpointId) {
        DeliveryEndpointRepository.StoredEndpoint stored;
        try {
            stored = repository.find(organizationId, endpointId)
                    .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
        } catch (DataAccessException unavailable) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
        }
        if (!"ACTIVE".equals(stored.status())) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        }
        try {
            if (stored.signingSecret() == null || stored.signingVersion() == null) {
                throw new IllegalStateException("Signing secret is unavailable");
            }
            String signing = decrypted(organizationId, endpointId, "signing_secret",
                    stored.signingVersion(), stored.signingSecret());
            JsonNode authentication = "NONE".equals(stored.authType())
                    ? json.readTree("{\"type\":\"NONE\"}")
                    : json.readTree(decrypted(organizationId, endpointId, "auth_config",
                            stored.authVersion(), stored.authSecret()));
            JsonNode query = json.readTree(stored.queryJson());
            JsonNode headers = json.readTree(stored.headersJson());
            if (authentication == null || !authentication.isObject() || query == null
                    || !query.isObject() || headers == null || !headers.isObject()) {
                throw new IllegalStateException("Endpoint configuration is invalid");
            }
            Configuration configuration = new Configuration(organizationId, endpointId, stored.version(),
                    stored.authVersion(), stored.credentialVersion(),
                    stored.baseUrl(), stored.path(), query, headers, stored.timeoutMs(),
                    stored.maxAttempts(), stored.maxConcurrency(), stored.organizationConcurrencyLimit(),
                    authentication, signing);
            if (json.writeValueAsBytes(configuration).length > 65536) {
                throw new IllegalStateException("Endpoint configuration is too large");
            }
            return configuration;
        } catch (IOException | RuntimeException unavailable) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
        }
    }

    private String decrypted(UUID organizationId, UUID endpointId, String field,
            Long version, EndpointSecretCipher.EncryptedSecret secret) {
        if (version == null || secret == null) {
            throw new IllegalStateException("Endpoint secret is unavailable");
        }
        byte[] plaintext = cipher.decrypt(organizationId, endpointId, field, version, secret);
        try {
            return new String(plaintext, StandardCharsets.UTF_8);
        } finally {
            Arrays.fill(plaintext, (byte) 0);
        }
    }

    public record Configuration(@JsonProperty("organization_id") UUID organizationId,
            @JsonProperty("endpoint_id") UUID endpointId, long version,
            @JsonProperty("auth_config_version") Long authConfigVersion,
            @JsonProperty("credential_version") long credentialVersion,
            @JsonProperty("base_url") String baseUrl, String path,
            @JsonProperty("query_params") JsonNode queryParams, JsonNode headers,
            @JsonProperty("timeout_ms") int timeoutMs,
            @JsonProperty("max_attempts") int maxAttempts,
            @JsonProperty("max_concurrency") int maxConcurrency,
            @JsonProperty("organization_concurrency_limit") int organizationConcurrencyLimit,
            JsonNode authentication, @JsonProperty("signing_secret") String signingSecret) {
        @Override
        public String toString() {
            return "Configuration[organizationId=" + organizationId + ", endpointId=" + endpointId
                    + ", version=" + version + ", destination=[REDACTED], authentication=[REDACTED], "
                    + "signingSecret=[REDACTED]]";
        }
    }
}

package com.relayforge.controlplane.endpoint;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.controlplane.endpoint.EndpointController.DeliveryPolicyRequest;
import com.relayforge.controlplane.endpoint.EndpointController.DestinationRequest;
import com.relayforge.controlplane.endpoint.EndpointRepository.EndpointUpdate;
import com.relayforge.controlplane.endpoint.EndpointRepository.AuthenticationState;
import com.relayforge.controlplane.endpoint.EndpointRepository.EndpointView;
import com.relayforge.controlplane.endpoint.EndpointRepository.SubscriptionView;
import com.relayforge.controlplane.endpoint.auth.EndpointAuthValidator;
import com.relayforge.controlplane.endpoint.auth.EndpointSecretCipher;
import com.relayforge.controlplane.endpoint.auth.EndpointSecretCipher.EncryptedSecret;
import com.relayforge.controlplane.endpoint.auth.InvalidAuthenticationException;
import com.relayforge.outbound.Destination;
import com.relayforge.outbound.DestinationPolicy;
import com.relayforge.outbound.InvalidDestinationException;
import com.relayforge.outbound.UnsafeDestinationException;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

public final class EndpointManagementService {
    private final EndpointRepository repository;
    private final TransactionTemplate transactions;
    private final EndpointChangeRecorder changes;
    private final DestinationPolicy policy;
    private final EndpointAuthValidator authValidator;
    private final EndpointSecretCipher secretCipher;
    private final ObjectMapper json;
    private final SecureRandom random;

    public EndpointManagementService(EndpointRepository repository, TransactionTemplate transactions,
            EndpointChangeRecorder changes, DestinationPolicy policy, EndpointAuthValidator authValidator,
            EndpointSecretCipher secretCipher, ObjectMapper json, SecureRandom random) {
        this.repository = repository;
        this.transactions = transactions;
        this.changes = changes;
        this.policy = policy;
        this.authValidator = authValidator;
        this.secretCipher = secretCipher;
        this.json = json;
        this.random = random;
    }

    public EndpointView get(UUID organizationId, UUID endpointId) {
        return repository.findForOrganization(organizationId, endpointId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
    }

    public Page<EndpointView> endpoints(UUID organizationId, EndpointQueryParameters.Page request) {
        return page(repository.listForOrganization(organizationId, request.cursor(), request.limit() + 1),
                request.limit(), EndpointView::id);
    }

    public Page<SubscriptionView> subscriptions(UUID organizationId, EndpointQueryParameters.Page request) {
        return page(repository.listSubscriptions(organizationId, request.cursor(), request.limit() + 1),
                request.limit(), SubscriptionView::id);
    }

    public EndpointView patch(UUID organizationId, UUID endpointId, long expectedVersion,
            EndpointController.PatchRequest body, String requestId) {
        if (body == null || expectedVersion == Long.MAX_VALUE || (body.name() == null && body.status() == null
                && body.destination() == null && body.deliveryPolicy() == null
                && body.authentication() == null)) {
            throw invalid();
        }
        String name = body.name() == null ? null : body.name().trim();
        if (name != null && (name.isBlank() || name.length() > 160)) {
            throw invalid();
        }
        String status = body.status();
        if (status != null && !List.of("ACTIVE", "PAUSED", "DISABLED").contains(status)) {
            throw invalid();
        }
        DestinationRequest destination = body.destination();
        String baseUrl = null;
        String path = null;
        String queryJson = null;
        String headersJson = null;
        String metadataJson = null;
        if (destination != null) {
            try {
                var validated = policy.validate(new Destination(destination.baseUrl(), destination.path(),
                        destination.queryParams(), destination.headers()));
                baseUrl = destination.baseUrl();
                path = destination.path();
                queryJson = encode(destination.queryParams());
                headersJson = encode(validated.headers());
                metadataJson = encode(destination.metadata() == null ? Map.of() : destination.metadata());
                if (metadataJson.length() > 8192) {
                    throw invalid();
                }
            } catch (InvalidDestinationException invalidDestination) {
                throw invalid();
            } catch (UnsafeDestinationException unsafeDestination) {
                throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY);
            }
        }
        DeliveryPolicyRequest delivery = body.deliveryPolicy();
        if (delivery != null && (!between(delivery.timeoutMs(), 100, 60000)
                || !between(delivery.maxAttempts(), 1, 20)
                || !between(delivery.maxConcurrency(), 1, 1000))) {
            throw invalid();
        }
        String authType = null;
        EncryptedSecret encryptedAuth = null;
        if (body.authentication() != null) {
            try {
                var authentication = authValidator.validate(body.authentication());
                authType = authentication.type();
                if (authentication.plaintextJson() != null) {
                    byte[] plaintext = authentication.plaintextJson().getBytes(StandardCharsets.UTF_8);
                    try {
                        encryptedAuth = secretCipher.encrypt(organizationId, endpointId, "auth_config",
                                expectedVersion + 1, plaintext);
                    } finally {
                        Arrays.fill(plaintext, (byte) 0);
                    }
                }
            } catch (InvalidAuthenticationException invalidAuthentication) {
                throw invalid();
            }
        }
        if (destination != null || body.authentication() != null) {
            AuthenticationState current = repository.findAuthentication(organizationId, endpointId)
                    .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
            String effectiveType = body.authentication() == null ? current.type() : authType;
            if (effectiveType.equals("STATIC_HEADER") || effectiveType.equals("STATIC_API_KEY")) {
                String headerName = body.authentication() == null
                        ? currentStaticHeaderName(organizationId, endpointId, current)
                        : body.authentication().path("header_name").asText();
                String effectiveHeaders = headersJson == null ? current.headersJson() : headersJson;
                if (hasHeader(effectiveHeaders, headerName)) {
                    throw invalid();
                }
            }
        }
        EndpointUpdate update = new EndpointUpdate(name, status, baseUrl, path, queryJson, headersJson,
                metadataJson, delivery == null ? null : delivery.timeoutMs(),
                delivery == null ? null : delivery.maxConcurrency(),
                delivery == null ? null : delivery.maxAttempts(), body.authentication() != null,
                authType, encryptedAuth);
        return transactions.execute(transaction -> {
            repository.update(organizationId, endpointId, expectedVersion, update)
                    .orElseThrow(() -> missingOrStale(organizationId, endpointId));
            repository.insertAudit(organizationId, endpointId,
                    "DISABLED".equals(status) ? "endpoint.disabled" : "endpoint.updated", requestId);
            changes.record(organizationId, endpointId);
            return get(organizationId, endpointId);
        });
    }

    public RotatedSecret rotateSecret(UUID organizationId, UUID endpointId, long expectedVersion,
            String requestId) {
        if (expectedVersion == Long.MAX_VALUE) {
            throw invalid();
        }
        byte[] randomBytes = new byte[32];
        random.nextBytes(randomBytes);
        String secret = Base64.getUrlEncoder().withoutPadding().encodeToString(randomBytes);
        Arrays.fill(randomBytes, (byte) 0);
        byte[] plaintext = secret.getBytes(StandardCharsets.US_ASCII);
        EncryptedSecret encrypted;
        try {
            encrypted = secretCipher.encrypt(organizationId, endpointId, "signing_secret",
                    expectedVersion + 1, plaintext);
        } finally {
            Arrays.fill(plaintext, (byte) 0);
        }
        long version = transactions.execute(transaction -> {
            long updated = repository.rotateSigningSecret(organizationId, endpointId, expectedVersion, encrypted)
                    .orElseThrow(() -> missingOrStale(organizationId, endpointId));
            repository.insertAudit(organizationId, endpointId, "endpoint.secret_rotated", requestId);
            changes.record(organizationId, endpointId);
            return updated;
        });
        return new RotatedSecret(endpointId, secret, version);
    }

    private ResponseStatusException missingOrStale(UUID organizationId, UUID endpointId) {
        return new ResponseStatusException(repository.findForOrganization(organizationId, endpointId).isPresent()
                ? HttpStatus.PRECONDITION_FAILED : HttpStatus.NOT_FOUND);
    }

    private String encode(Object value) {
        try {
            return json.writeValueAsString(value);
        } catch (JsonProcessingException failure) {
            throw invalid();
        }
    }

    private String currentStaticHeaderName(UUID organizationId, UUID endpointId,
            AuthenticationState current) {
        if (current.secret() == null || current.version() == null) {
            throw new IllegalStateException("Stored authentication is unavailable");
        }
        byte[] plaintext = secretCipher.decrypt(organizationId, endpointId, "auth_config",
                current.version(), current.secret());
        try {
            JsonNode configuration = json.readTree(plaintext);
            return configuration.path("header_name").asText();
        } catch (IOException failure) {
            throw new IllegalStateException("Stored authentication is invalid", failure);
        } finally {
            Arrays.fill(plaintext, (byte) 0);
        }
    }

    private boolean hasHeader(String headersJson, String name) {
        try {
            var names = json.readTree(headersJson).fieldNames();
            while (names.hasNext()) {
                if (names.next().equalsIgnoreCase(name)) {
                    return true;
                }
            }
            return false;
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("Stored destination headers are invalid", failure);
        }
    }

    private boolean between(Integer value, int minimum, int maximum) {
        return value != null && value >= minimum && value <= maximum;
    }

    private ResponseStatusException invalid() {
        return new ResponseStatusException(HttpStatus.BAD_REQUEST);
    }

    private <T> Page<T> page(List<T> rows, int limit, java.util.function.Function<T, UUID> id) {
        boolean hasNext = rows.size() > limit;
        List<T> data = hasNext ? rows.subList(0, limit) : rows;
        UUID nextCursor = hasNext ? id.apply(data.getLast()) : null;
        return new Page<>(List.copyOf(data), nextCursor);
    }

    public record Page<T>(List<T> data, UUID nextCursor) {
    }

    public record RotatedSecret(UUID endpointId, String secret, long version) {
        @Override
        public String toString() {
            return "RotatedSecret[endpointId=" + endpointId + ", secret=[REDACTED], version=" + version + "]";
        }
    }
}

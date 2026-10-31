package com.relayforge.controlplane.endpoint;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.controlplane.endpoint.EndpointController.DeliveryPolicyRequest;
import com.relayforge.controlplane.endpoint.EndpointController.DestinationRequest;
import com.relayforge.controlplane.endpoint.EndpointController.EndpointRequest;
import com.relayforge.controlplane.endpoint.auth.EndpointAuthValidator;
import com.relayforge.controlplane.endpoint.auth.EndpointSecretCipher;
import com.relayforge.controlplane.endpoint.auth.EndpointSecretCipher.EncryptedSecret;
import com.relayforge.controlplane.endpoint.auth.InvalidAuthenticationException;
import com.relayforge.outbound.Destination;
import com.relayforge.outbound.DestinationPolicy;
import com.relayforge.outbound.InvalidDestinationException;
import com.relayforge.outbound.UnsafeDestinationException;
import com.relayforge.outbound.ValidatedDestination;
import java.time.Clock;
import java.time.Instant;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

public final class EndpointRegistrationService {
    private final EndpointRepository repository;
    private final DestinationPolicy policy;
    private final TransactionTemplate transactions;
    private final ObjectMapper json;
    private final Clock clock;
    private final EndpointAuthValidator authValidator;
    private final EndpointSecretCipher secretCipher;
    private final EndpointChangeRecorder changes;

    public EndpointRegistrationService(EndpointRepository repository, DestinationPolicy policy,
            TransactionTemplate transactions, ObjectMapper json, Clock clock,
            EndpointAuthValidator authValidator, EndpointSecretCipher secretCipher,
            EndpointChangeRecorder changes) {
        this.repository = repository;
        this.policy = policy;
        this.transactions = transactions;
        this.json = json;
        this.clock = clock;
        this.authValidator = authValidator;
        this.secretCipher = secretCipher;
        this.changes = changes;
    }

    public CreatedEndpoint create(UUID organizationId, EndpointRequest body, String requestId) {
        if (organizationId == null || body == null || body.destination() == null
                || body.authentication() == null) {
            throw invalid();
        }
        String name = body.name();
        if (name == null || name.isBlank() || name.length() > 160) {
            throw invalid();
        }
        DestinationRequest destination = body.destination();
        Map<String, Object> metadata = destination.metadata() == null ? Map.of() : destination.metadata();
        String metadataJson = toJson(metadata);
        if (metadataJson.length() > 8192) {
            throw invalid();
        }
        List<String> eventTypes = body.eventTypes();
        if (eventTypes == null || eventTypes.isEmpty() || eventTypes.size() > 100
                || new HashSet<>(eventTypes).size() != eventTypes.size()
                || eventTypes.stream().anyMatch(value -> value == null
                        || !value.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,199}"))) {
            throw invalid();
        }
        DeliveryPolicyRequest deliveryPolicy = body.deliveryPolicy();
        if (deliveryPolicy == null || !between(deliveryPolicy.timeoutMs(), 100, 60000)
                || !between(deliveryPolicy.maxAttempts(), 1, 20)
                || !between(deliveryPolicy.maxConcurrency(), 1, 1000)) {
            throw invalid();
        }
        ValidatedDestination validated;
        EndpointAuthValidator.ValidatedAuthentication authentication;
        try {
            validated = policy.validate(new Destination(destination.baseUrl(), destination.path(),
                    destination.queryParams(), destination.headers()));
            authentication = authValidator.validate(body.authentication());
        } catch (InvalidDestinationException invalidDestination) {
            throw invalid();
        } catch (InvalidAuthenticationException invalidAuthentication) {
            throw invalid();
        } catch (UnsafeDestinationException unsafeDestination) {
            throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY);
        }
        if ((authentication.type().equals("STATIC_HEADER")
                || authentication.type().equals("STATIC_API_KEY"))
                && validated.headers().keySet().stream().anyMatch(headerName -> headerName.equalsIgnoreCase(
                        body.authentication().path("header_name").asText()))) {
            throw invalid();
        }
        UUID endpointId = UUID.randomUUID();
        Instant createdAt = clock.instant();
        EncryptedSecret encryptedAuth = null;
        if (authentication.plaintextJson() != null) {
            byte[] plaintext = authentication.plaintextJson().getBytes(StandardCharsets.UTF_8);
            try {
                encryptedAuth = secretCipher.encrypt(organizationId, endpointId, "auth_config", 1, plaintext);
            } finally {
                Arrays.fill(plaintext, (byte) 0);
            }
        }
        String queryJson = toJson(destination.queryParams());
        String headersJson = toJson(validated.headers());
        EncryptedSecret storedAuth = encryptedAuth;
        return transactions.execute(status -> {
            repository.insert(endpointId, organizationId, name.trim(), destination.baseUrl(),
                    destination.path(), queryJson, headersJson, metadataJson,
                    deliveryPolicy.timeoutMs(), deliveryPolicy.maxConcurrency(), deliveryPolicy.maxAttempts(),
                    createdAt, authentication.type(), storedAuth);
            for (String eventType : eventTypes) {
                repository.insertSubscription(UUID.randomUUID(), organizationId, endpointId, eventType);
            }
            repository.insertAudit(organizationId, endpointId, "endpoint.created", requestId);
            changes.record(organizationId, endpointId);
            return new CreatedEndpoint(endpointId, name.trim(), List.copyOf(eventTypes), createdAt);
        });
    }

    public EndpointRepository.EndpointView get(UUID organizationId, UUID endpointId) {
        return repository.findForOrganization(organizationId, endpointId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
    }

    private boolean between(Integer value, int minimum, int maximum) {
        return value != null && value >= minimum && value <= maximum;
    }

    private String toJson(Object value) {
        try {
            return json.writeValueAsString(value);
        } catch (JsonProcessingException invalidJson) {
            throw invalid();
        }
    }

    private ResponseStatusException invalid() {
        return new ResponseStatusException(HttpStatus.BAD_REQUEST);
    }

    public record CreatedEndpoint(UUID id, String name, List<String> eventTypes, Instant createdAt) {
    }
}

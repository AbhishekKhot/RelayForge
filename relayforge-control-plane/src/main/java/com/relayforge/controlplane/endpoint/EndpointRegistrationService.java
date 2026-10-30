package com.relayforge.controlplane.endpoint;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.controlplane.endpoint.EndpointController.DeliveryPolicyRequest;
import com.relayforge.controlplane.endpoint.EndpointController.DestinationRequest;
import com.relayforge.controlplane.endpoint.EndpointController.EndpointRequest;
import com.relayforge.outbound.Destination;
import com.relayforge.outbound.DestinationPolicy;
import com.relayforge.outbound.InvalidDestinationException;
import com.relayforge.outbound.UnsafeDestinationException;
import com.relayforge.outbound.ValidatedDestination;
import java.time.Clock;
import java.time.Instant;
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

    public EndpointRegistrationService(EndpointRepository repository, DestinationPolicy policy,
            TransactionTemplate transactions, ObjectMapper json, Clock clock) {
        this.repository = repository;
        this.policy = policy;
        this.transactions = transactions;
        this.json = json;
        this.clock = clock;
    }

    public CreatedEndpoint create(UUID organizationId, EndpointRequest body, String requestId) {
        if (organizationId == null || body == null || body.destination() == null
                || body.authentication() == null || !"NONE".equals(body.authentication().type())) {
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
        try {
            validated = policy.validate(new Destination(destination.baseUrl(), destination.path(),
                    destination.queryParams(), destination.headers()));
        } catch (InvalidDestinationException invalidDestination) {
            throw invalid();
        } catch (UnsafeDestinationException unsafeDestination) {
            throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY);
        }
        UUID endpointId = UUID.randomUUID();
        Instant createdAt = clock.instant();
        String queryJson = toJson(destination.queryParams());
        String headersJson = toJson(validated.headers());
        return transactions.execute(status -> {
            repository.insert(endpointId, organizationId, name.trim(), destination.baseUrl(),
                    destination.path(), queryJson, headersJson, metadataJson,
                    deliveryPolicy.timeoutMs(), deliveryPolicy.maxConcurrency(), deliveryPolicy.maxAttempts(),
                    createdAt);
            for (String eventType : eventTypes) {
                repository.insertSubscription(UUID.randomUUID(), organizationId, endpointId, eventType);
            }
            repository.insertAudit(organizationId, endpointId, requestId);
            return new CreatedEndpoint(endpointId, name.trim(), List.copyOf(eventTypes), createdAt);
        });
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

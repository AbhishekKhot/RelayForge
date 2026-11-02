package com.relayforge.ingestion.event;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Objects;
import java.util.UUID;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public final class DeliveryPayloadLookupService {
    private final RoutingEventLookupService references;
    private final IngestionRepository repository;
    private final ObjectMapper json;

    public DeliveryPayloadLookupService(RoutingEventLookupService references,
            IngestionRepository repository, ObjectMapper json) {
        this.references = Objects.requireNonNull(references);
        this.repository = Objects.requireNonNull(repository);
        this.json = Objects.requireNonNull(json);
    }

    public Payload find(UUID organizationId, String locator) {
        RoutingEventLookupService.VerifiedEvent verified = references.find(organizationId, locator);
        try {
            IngestionRepository.StoredEvent event = repository.findEvent(organizationId,
                    verified.acceptedDay(), verified.eventId())
                    .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
            if (event.payloadJson() == null || !event.eventType().equals(verified.eventType())
                    || !event.acceptedAt().equals(verified.acceptedAt())
                    || event.payloadJson().getBytes(StandardCharsets.UTF_8).length > 262144) {
                throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
            }
            JsonNode data = json.readTree(event.payloadJson());
            if (data == null || !data.isObject()) {
                throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
            }
            return new Payload(organizationId, verified.eventId(), locator,
                    verified.acceptedDay(), verified.acceptedAt(), verified.eventType(),
                    event.schemaVersion(), data);
        } catch (IOException | DataAccessException unavailable) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
        }
    }

    public record Payload(@JsonProperty("organization_id") UUID organizationId,
            @JsonProperty("event_id") UUID eventId, String locator,
            @JsonProperty("accepted_day") LocalDate acceptedDay,
            @JsonProperty("accepted_at") Instant acceptedAt,
            @JsonProperty("event_type") String eventType,
            @JsonProperty("schema_version") int schemaVersion, JsonNode payload) {
        @Override
        public String toString() {
            return "Payload[organizationId=" + organizationId + ", eventId=" + eventId
                    + ", payload=[REDACTED]]";
        }
    }
}

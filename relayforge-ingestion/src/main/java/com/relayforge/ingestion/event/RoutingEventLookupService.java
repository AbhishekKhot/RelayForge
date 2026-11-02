package com.relayforge.ingestion.event;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.time.LocalDate;
import java.util.HexFormat;
import java.util.Objects;
import java.util.UUID;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public final class RoutingEventLookupService {
    private final IngestionRepository repository;
    private final String ingestionShardId;
    private final String deliveryShardId;

    public RoutingEventLookupService(IngestionRepository repository, String ingestionShardId,
            String deliveryShardId) {
        this.repository = Objects.requireNonNull(repository);
        this.ingestionShardId = Objects.requireNonNull(ingestionShardId);
        this.deliveryShardId = Objects.requireNonNull(deliveryShardId);
    }

    public VerifiedEvent find(UUID organizationId, String locator) {
        EventLocator.Decoded decoded;
        try {
            decoded = EventLocator.decode(locator);
        } catch (IllegalArgumentException invalid) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        }
        if (!ingestionShardId.equals(decoded.ingestionShardId())
                || !deliveryShardId.equals(decoded.deliveryShardId())) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        }
        try {
            IngestionRepository.RoutingEvent stored = repository.findRoutingEvent(organizationId,
                    decoded.acceptedDay(), decoded.eventId(), locator)
                    .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
            return new VerifiedEvent(organizationId, decoded.eventId(), decoded.acceptedDay(),
                    stored.acceptedAt(), stored.eventType(),
                    HexFormat.of().formatHex(stored.payloadHash()));
        } catch (DataAccessException unavailable) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
        }
    }

    public record VerifiedEvent(@JsonProperty("organization_id") UUID organizationId,
            @JsonProperty("event_id") UUID eventId,
            @JsonProperty("accepted_day") LocalDate acceptedDay,
            @JsonProperty("accepted_at") Instant acceptedAt,
            @JsonProperty("event_type") String eventType,
            @JsonProperty("payload_hash") String payloadHash) {
    }
}

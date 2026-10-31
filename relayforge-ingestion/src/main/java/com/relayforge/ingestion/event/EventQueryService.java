package com.relayforge.ingestion.event;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.UUID;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public final class EventQueryService {
    private final IngestionRepository repository;
    private final DeliverySummaryClient summaries;
    private final ObjectMapper json;
    private final String ingestionShardId;
    private final String deliveryShardId;

    public EventQueryService(IngestionRepository repository, DeliverySummaryClient summaries,
            ObjectMapper json, String ingestionShardId, String deliveryShardId) {
        this.repository = repository;
        this.summaries = summaries;
        this.json = json;
        this.ingestionShardId = ingestionShardId;
        this.deliveryShardId = deliveryShardId;
    }

    public QueriedEvent find(UUID organizationId, String locator) {
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
            IngestionRepository.StoredEvent stored = repository.findEvent(organizationId,
                    decoded.acceptedDay(), decoded.eventId())
                    .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
            if (stored.payloadJson() == null) {
                throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
            }
            JsonNode payload = json.readTree(stored.payloadJson());
            if (payload == null || !payload.isObject()) {
                throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
            }
            DeliverySummary summary = summaries.read(organizationId, decoded.acceptedDay(), decoded.eventId());
            return new QueriedEvent(locator, stored.eventType(), stored.schemaVersion(), payload,
                    stored.occurredAt(), stored.acceptedAt(), summary);
        } catch (DataAccessException | JsonProcessingException unavailable) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
        }
    }

    public record QueriedEvent(String id, String eventType, int schemaVersion,
            JsonNode payload, Instant occurredAt, Instant acceptedAt, DeliverySummary deliverySummary) {
    }
}

package com.relayforge.ingestion.event;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.ingestion.event.OrganizationAuthenticator.AuthenticatedOrganization;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

public final class EventIngestionService {
    private final IngestionRepository repository;
    private final DurabilityGate durability;
    private final TransactionTemplate transactions;
    private final ObjectMapper json;
    private final long mapEpoch;
    private final String ingestionShardId;
    private final String deliveryShardId;

    public EventIngestionService(IngestionRepository repository, DurabilityGate durability,
            TransactionTemplate transactions, ObjectMapper json, long mapEpoch,
            String ingestionShardId, String deliveryShardId) {
        this.repository = repository;
        this.durability = durability;
        this.transactions = transactions;
        this.json = json;
        this.mapEpoch = mapEpoch;
        this.ingestionShardId = ingestionShardId;
        this.deliveryShardId = deliveryShardId;
    }

    public AcceptedEvent accept(AuthenticatedOrganization organization, String key, EventPayload payload) {
        int bucket;
        try {
            bucket = IdempotencyBucket.forKey(organization.id(), key);
        } catch (IllegalArgumentException invalid) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        }
        try {
            durability.verify();
            return transactions.execute(status -> {
                durability.verify();
                if (!repository.activeFence(bucket, mapEpoch)) {
                    throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
                }
                IngestionRepository.RegistryEntry existing = repository.lockRegistry(organization.id(), key)
                        .orElse(null);
                if (existing != null) {
                    return resolveExisting(organization, key, bucket, payload, existing);
                }
                Instant acceptedAt = repository.databaseTime();
                UUID eventId = UUID.randomUUID();
                LocalDate day = acceptedAt.atZone(ZoneOffset.UTC).toLocalDate();
                String locator = EventLocator.encode(eventId, day, bucket, mapEpoch,
                        ingestionShardId, deliveryShardId);
                if (!repository.insertRegistry(organization.id(), key, bucket, payload.requestHash(),
                        eventId, locator, day, acceptedAt)) {
                    IngestionRepository.RegistryEntry raced = repository.lockRegistry(organization.id(), key)
                            .orElseThrow(() -> new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE));
                    return resolveExisting(organization, key, bucket, payload, raced);
                }
                writeNew(organization, key, bucket, payload, eventId, locator, day, acceptedAt);
                return new AcceptedEvent(locator, payload.eventType(), acceptedAt);
            });
        } catch (DataAccessException | TransactionException unavailable) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
        }
    }

    private AcceptedEvent resolveExisting(AuthenticatedOrganization organization, String key, int bucket,
            EventPayload payload, IngestionRepository.RegistryEntry existing) {
        Instant now = repository.databaseTime();
        if (existing.expiresAt().isAfter(now)) {
            if (!MessageDigest.isEqual(existing.requestHash(), payload.requestHash())) {
                throw new ResponseStatusException(HttpStatus.CONFLICT);
            }
            return new AcceptedEvent(existing.locator(), payload.eventType(), existing.acceptedAt());
        }
        UUID eventId = UUID.randomUUID();
        LocalDate day = now.atZone(ZoneOffset.UTC).toLocalDate();
        String locator = EventLocator.encode(eventId, day, bucket, mapEpoch,
                ingestionShardId, deliveryShardId);
        repository.replaceRegistry(organization.id(), key, bucket, payload.requestHash(),
                eventId, locator, day, now);
        writeNew(organization, key, bucket, payload, eventId, locator, day, now);
        return new AcceptedEvent(locator, payload.eventType(), now);
    }

    private void writeNew(AuthenticatedOrganization organization, String key, int bucket,
            EventPayload payload, UUID eventId, String locator, LocalDate day, Instant acceptedAt) {
        if (!repository.reserveRate(organization.id(), acceptedAt, organization.eventsPerMinute())) {
            throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS);
        }
        repository.insertEvent(organization.id(), key, eventId, day, acceptedAt, payload);
        Map<String, Object> message = new LinkedHashMap<>();
        message.put("organization_id", organization.id().toString());
        message.put("event_id", locator);
        message.put("accepted_day", day.toString());
        message.put("accepted_at", acceptedAt.toString());
        message.put("ingestion_locator_version", 1);
        message.put("bucket_id", bucket);
        message.put("map_epoch", mapEpoch);
        message.put("ingestion_shard_id", ingestionShardId);
        message.put("delivery_shard_id", deliveryShardId);
        message.put("event_type", payload.eventType());
        message.put("payload_hash", HexFormat.of().formatHex(payload.payloadHash()));
        try {
            repository.insertOutbox(organization.id(), eventId, day, locator,
                    json.writeValueAsString(message));
        } catch (JsonProcessingException impossible) {
            throw new IllegalStateException("Could not encode event envelope", impossible);
        }
    }

    public record AcceptedEvent(String id, String eventType, Instant acceptedAt) {
    }
}

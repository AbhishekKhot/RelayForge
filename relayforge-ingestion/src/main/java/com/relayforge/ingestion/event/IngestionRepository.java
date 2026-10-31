package com.relayforge.ingestion.event;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Optional;
import java.util.UUID;

public interface IngestionRepository {
    boolean activeFence(int bucketId, long epoch);

    Optional<RegistryEntry> lockRegistry(UUID organizationId, String key);

    Instant databaseTime();

    boolean insertRegistry(UUID organizationId, String key, int bucketId, byte[] requestHash,
            UUID eventId, String locator, LocalDate day, Instant acceptedAt);

    void replaceRegistry(UUID organizationId, String key, int bucketId, byte[] requestHash,
            UUID eventId, String locator, LocalDate day, Instant acceptedAt);

    boolean reserveRate(UUID organizationId, Instant acceptedAt, int limit);

    int deleteExpiredRateCounters(int maximumRows);

    void insertEvent(UUID organizationId, String key, UUID eventId, LocalDate day,
            Instant acceptedAt, EventPayload payload);

    void insertOutbox(UUID organizationId, UUID eventId, LocalDate day, String locator,
            String messageJson);

    Optional<StoredEvent> findEvent(UUID organizationId, LocalDate day, UUID eventId);

    record StoredEvent(String eventType, int schemaVersion, String payloadJson,
            Instant occurredAt, Instant acceptedAt) {
    }

    record RegistryEntry(byte[] requestHash, String locator, UUID eventId, Instant acceptedAt,
            Instant expiresAt) {
        @Override
        public byte[] requestHash() {
            return requestHash.clone();
        }
    }
}

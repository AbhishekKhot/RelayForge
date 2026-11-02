package com.relayforge.router.routing;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.UUID;

public record VerifiedEvent(UUID organizationId, UUID eventId, LocalDate acceptedDay,
        Instant acceptedAt, String eventType, String ingestionShardId, String deliveryShardId) {
    public VerifiedEvent {
        Objects.requireNonNull(organizationId);
        Objects.requireNonNull(eventId);
        Objects.requireNonNull(acceptedDay);
        Objects.requireNonNull(acceptedAt);
        if (!acceptedDay.equals(acceptedAt.atZone(ZoneOffset.UTC).toLocalDate())
                || eventType == null || eventType.isBlank() || eventType.length() > 200
                || ingestionShardId == null || ingestionShardId.isBlank()
                || deliveryShardId == null || deliveryShardId.isBlank()) {
            throw new IllegalArgumentException("Invalid verified event");
        }
    }
}

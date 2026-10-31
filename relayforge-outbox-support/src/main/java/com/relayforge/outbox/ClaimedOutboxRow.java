package com.relayforge.outbox;

import java.time.Instant;
import java.util.UUID;

public record ClaimedOutboxRow(UUID id, UUID organizationId, String aggregateId,
        String messageType, String topic, String partitionKey, String payloadJson,
        UUID claimToken, int publishAttempts, Instant createdAt) {
}

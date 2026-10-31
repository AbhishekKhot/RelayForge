package com.relayforge.contracts;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

public record ValidatedEnvelope(String messageId, String messageType, int schemaVersion,
        Instant occurredAt, UUID organizationId, String aggregateId, String correlationId,
        JsonNode payload) {
}

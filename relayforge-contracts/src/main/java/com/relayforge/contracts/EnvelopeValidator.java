package com.relayforge.contracts;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public final class EnvelopeValidator {
    private static final int MAX_ENVELOPE_BYTES = 131072;
    private static final Map<String, String> TYPES = Map.of(
            "relayforge.config.changed.v1", "configuration.changed",
            "relayforge.event.accepted.v1", "event.accepted",
            "relayforge.delivery.ready.v1", "delivery.ready",
            "relayforge.analytics.delivery-facts.v1", "analytics.delivery-facts");
    private final ObjectMapper json;

    public EnvelopeValidator(ObjectMapper json) {
        this.json = Objects.requireNonNull(json);
    }

    public ValidatedEnvelope validate(String topic, String key, String value) {
        String expectedType = TYPES.get(topic);
        if (expectedType == null) {
            throw invalid("UNKNOWN_TOPIC");
        }
        if (value == null || value.getBytes(StandardCharsets.UTF_8).length > MAX_ENVELOPE_BYTES) {
            throw invalid("OVERSIZED_ENVELOPE");
        }
        JsonNode root;
        try {
            root = json.readTree(value);
        } catch (JsonProcessingException malformed) {
            throw invalid("MALFORMED_JSON");
        }
        if (root == null || !root.isObject()) {
            throw invalid("INVALID_ENVELOPE");
        }
        String type = requiredText(root, "message_type", 100);
        JsonNode versionNode = root.path("schema_version");
        int version = versionNode.isIntegralNumber() ? versionNode.asInt(-1) : -1;
        if (version != 1 || (type.startsWith(expectedType + ".v")
                && !type.equals(expectedType + ".v1"))) {
            throw invalid("UNSUPPORTED_VERSION");
        }
        if (!type.equals(expectedType + ".v1")) {
            throw invalid("MESSAGE_TYPE_MISMATCH");
        }
        String messageId = requiredText(root, "message_id", 50);
        if (!messageId.startsWith("msg_") || !isUuid(messageId.substring(4))) {
            throw invalid("INVALID_ENVELOPE");
        }
        UUID organizationId = parseUuid(requiredText(root, "organization_id", 50));
        String aggregateId = requiredText(root, "aggregate_id", 300);
        String correlationId = requiredText(root, "correlation_id", 200);
        Instant occurredAt = parseInstant(requiredText(root, "occurred_at", 40));
        JsonNode payload = root.path("payload");
        if (!payload.isObject()) {
            throw invalid("INVALID_PAYLOAD");
        }
        if (payload.has("organization_id")
                && !organizationId.toString().equals(payload.path("organization_id").asText())) {
            throw invalid("INVALID_PAYLOAD");
        }
        validatePayload(topic, key, aggregateId, organizationId, payload);
        return new ValidatedEnvelope(messageId, type, version, occurredAt, organizationId,
                aggregateId, correlationId, payload.deepCopy());
    }

    private void validatePayload(String topic, String key, String aggregateId,
            UUID organizationId, JsonNode payload) {
        if (key == null || key.isBlank() || key.length() > 300) {
            throw invalid("INVALID_PAYLOAD");
        }
        switch (topic) {
            case "relayforge.config.changed.v1" -> {
                if (!organizationId.toString().equals(key)
                        || !organizationId.toString().equals(payload.path("organization_id").asText())
                        || !payload.path("source_revision").isIntegralNumber()
                        || payload.path("source_revision").asLong(-1) < 1
                        || !payload.path("endpoint").isObject()
                        || !aggregateId.equals(payload.path("endpoint").path("endpoint_id").asText())
                        || !payload.path("subscriptions").isArray()) {
                    throw invalid("INVALID_PAYLOAD");
                }
            }
            case "relayforge.event.accepted.v1" -> {
                if (!key.startsWith("evt_")
                        || !organizationId.toString().equals(payload.path("organization_id").asText())
                        || !key.equals(payload.path("event_id").asText())
                        || !isUuid(aggregateId)
                        || payload.path("ingestion_locator_version").asInt(-1) != 1
                        || requiredPayloadText(payload, "accepted_day").isBlank()
                        || requiredPayloadText(payload, "accepted_at").isBlank()
                        || requiredPayloadText(payload, "event_type").isBlank()
                        || requiredPayloadText(payload, "ingestion_shard_id").isBlank()
                        || requiredPayloadText(payload, "delivery_shard_id").isBlank()
                        || requiredPayloadText(payload, "payload_hash").isBlank()) {
                    throw invalid("INVALID_PAYLOAD");
                }
            }
            case "relayforge.delivery.ready.v1" -> {
                if (!key.equals(aggregateId)
                        || !key.equals(payload.path("delivery_id").asText())) {
                    throw invalid("INVALID_PAYLOAD");
                }
            }
            case "relayforge.analytics.delivery-facts.v1" -> {
                if (!key.equals(payload.path("fact_id").asText())
                        || !key.equals(aggregateId)
                        || !organizationId.toString().equals(payload.path("organization_id").asText())) {
                    throw invalid("INVALID_PAYLOAD");
                }
            }
            default -> throw invalid("UNKNOWN_TOPIC");
        }
    }

    private String requiredPayloadText(JsonNode payload, String name) {
        JsonNode value = payload.path(name);
        return value.isTextual() ? value.asText() : "";
    }

    private String requiredText(JsonNode node, String name, int maxLength) {
        JsonNode field = node.path(name);
        if (!field.isTextual() || field.asText().isBlank() || field.asText().length() > maxLength) {
            throw invalid("INVALID_ENVELOPE");
        }
        return field.asText();
    }

    private UUID parseUuid(String value) {
        try {
            UUID parsed = UUID.fromString(value);
            if (value.length() != 36 || !parsed.toString().equalsIgnoreCase(value)) {
                throw invalid("INVALID_ENVELOPE");
            }
            return parsed;
        } catch (IllegalArgumentException malformed) {
            throw invalid("INVALID_ENVELOPE");
        }
    }

    private boolean isUuid(String value) {
        try {
            return value.length() == 36 && UUID.fromString(value).toString().equalsIgnoreCase(value);
        } catch (IllegalArgumentException malformed) {
            return false;
        }
    }

    private Instant parseInstant(String value) {
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException malformed) {
            throw invalid("INVALID_ENVELOPE");
        }
    }

    private InvalidEnvelopeException invalid(String reason) {
        return new InvalidEnvelopeException(reason);
    }
}

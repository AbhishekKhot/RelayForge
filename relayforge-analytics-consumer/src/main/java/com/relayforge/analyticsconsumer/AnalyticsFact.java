package com.relayforge.analyticsconsumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.relayforge.contracts.ValidatedEnvelope;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import java.util.UUID;

public record AnalyticsFact(UUID organizationId, LocalDate eventDate, UUID factId, long version,
        UUID eventId, UUID deliveryId, UUID endpointId, Integer replayGeneration,
        String factType, String status, Integer attemptNumber, Integer httpStatus,
        Integer durationMs, String errorCode, Instant occurredAt) {
    private static final Set<String> FIELDS = Set.of("organization_id", "event_date", "fact_id",
            "version", "event_id", "delivery_id", "endpoint_id", "replay_generation",
            "fact_type", "status", "attempt_number", "http_status", "duration_ms",
            "error_code", "occurred_at");
    private static final Set<String> TYPES = Set.of("event.accepted", "delivery.created",
            "delivery.attempt.finished", "delivery.succeeded", "delivery.retry_scheduled",
            "delivery.dead_lettered", "delivery.replayed");
    private static final DateTimeFormatter CLICKHOUSE_TIME =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    public static AnalyticsFact parse(ValidatedEnvelope envelope) {
        if (envelope == null || !"analytics.delivery-facts.v1".equals(envelope.messageType())) {
            throw invalid();
        }
        JsonNode node = envelope.payload();
        if (!node.isObject() || node.size() > FIELDS.size()) {
            throw invalid();
        }
        var names = node.fieldNames();
        while (names.hasNext()) {
            if (!FIELDS.contains(names.next())) {
                throw invalid();
            }
        }
        try {
            UUID organization = uuid(node, "organization_id", true);
            UUID fact = uuid(node, "fact_id", true);
            if (!organization.equals(envelope.organizationId())
                    || !fact.toString().equals(envelope.aggregateId())) {
                throw invalid();
            }
            long version = node.path("version").isIntegralNumber()
                    ? node.path("version").asLong(-1) : -1;
            String type = text(node, "fact_type", 80);
            String status = text(node, "status", 32);
            String error = nullableText(node, "error_code", 80);
            Instant occurred = Instant.parse(text(node, "occurred_at", 40));
            LocalDate date = LocalDate.parse(text(node, "event_date", 10));
            if (version < 1 || !TYPES.contains(type)
                    || !status.matches("[A-Z][A-Z0-9_]{0,31}")
                    || error != null && !error.matches("[A-Z][A-Z0-9_]{0,79}")
                    || !date.equals(occurred.atZone(ZoneOffset.UTC).toLocalDate())) {
                throw invalid();
            }
            return new AnalyticsFact(organization, date, fact, version,
                    uuid(node, "event_id", false), uuid(node, "delivery_id", false),
                    uuid(node, "endpoint_id", false), number(node, "replay_generation", 1, Integer.MAX_VALUE),
                    type, status, number(node, "attempt_number", 1, 65535),
                    number(node, "http_status", 100, 599), number(node, "duration_ms", 0, 3600000),
                    error == null ? "" : error, occurred);
        } catch (RuntimeException malformed) {
            throw invalid();
        }
    }

    public String json(ObjectMapper mapper) {
        ObjectNode node = mapper.createObjectNode();
        node.put("organization_id", organizationId.toString());
        node.put("event_date", eventDate.toString());
        node.put("fact_id", factId.toString());
        node.put("version", version);
        put(node, "event_id", eventId);
        put(node, "delivery_id", deliveryId);
        put(node, "endpoint_id", endpointId);
        put(node, "replay_generation", replayGeneration);
        node.put("fact_type", factType);
        node.put("status", status);
        put(node, "attempt_number", attemptNumber);
        put(node, "http_status", httpStatus);
        put(node, "duration_ms", durationMs);
        node.put("error_code", errorCode);
        node.put("occurred_at", CLICKHOUSE_TIME.format(occurredAt));
        return node.toString();
    }

    private static void put(ObjectNode node, String name, UUID value) {
        if (value == null) {
            node.putNull(name);
        } else {
            node.put(name, value.toString());
        }
    }

    private static void put(ObjectNode node, String name, Integer value) {
        if (value == null) {
            node.putNull(name);
        } else {
            node.put(name, value);
        }
    }

    private static UUID uuid(JsonNode node, String name, boolean required) {
        JsonNode value = node.path(name);
        if (value.isMissingNode() || value.isNull()) {
            if (!required) {
                return null;
            }
            throw invalid();
        }
        if (!value.isTextual() || value.asText().length() != 36) {
            throw invalid();
        }
        UUID parsed = UUID.fromString(value.asText());
        if (!parsed.toString().equalsIgnoreCase(value.asText())) {
            throw invalid();
        }
        return parsed;
    }

    private static Integer number(JsonNode node, String name, long minimum, long maximum) {
        JsonNode value = node.path(name);
        if (value.isMissingNode() || value.isNull()) {
            return null;
        }
        if (!value.isIntegralNumber() || value.asLong(-1) < minimum || value.asLong() > maximum) {
            throw invalid();
        }
        return value.intValue();
    }

    private static String text(JsonNode node, String name, int maximum) {
        JsonNode value = node.path(name);
        if (!value.isTextual() || value.asText().isBlank() || value.asText().length() > maximum) {
            throw invalid();
        }
        return value.asText();
    }

    private static String nullableText(JsonNode node, String name, int maximum) {
        JsonNode value = node.path(name);
        return value.isMissingNode() || value.isNull() ? null : text(node, name, maximum);
    }

    private static IllegalArgumentException invalid() {
        return new IllegalArgumentException("Invalid analytics fact");
    }
}

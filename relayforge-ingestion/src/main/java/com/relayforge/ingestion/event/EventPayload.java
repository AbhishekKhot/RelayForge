package com.relayforge.ingestion.event;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import org.erdtman.jcs.JsonCanonicalizer;

public final class EventPayload {
    private static final BigInteger MAX_EXACT_INTEGER = BigInteger.valueOf(9007199254740991L);
    private final String eventType;
    private final int schemaVersion;
    private final Instant occurredAt;
    private final String payloadJson;
    private final byte[] payloadHash;
    private final byte[] requestHash;

    private EventPayload(String eventType, int schemaVersion, Instant occurredAt,
            String payloadJson, byte[] payloadHash, byte[] requestHash) {
        this.eventType = eventType;
        this.schemaVersion = schemaVersion;
        this.occurredAt = occurredAt;
        this.payloadJson = payloadJson;
        this.payloadHash = payloadHash.clone();
        this.requestHash = requestHash.clone();
    }

    public static EventPayload validate(ObjectMapper json, String eventType, Integer schemaVersion,
            Instant occurredAt, JsonNode data, int maxPayloadBytes) {
        int resolvedVersion = schemaVersion == null ? 1 : schemaVersion;
        if (eventType == null || !eventType.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,199}")
                || resolvedVersion < 1 || resolvedVersion > 1000
                || data == null || !data.isObject() || maxPayloadBytes < 1) {
            throw new IllegalArgumentException("Invalid event");
        }
        validateJson(data, 1);
        try {
            byte[] canonicalPayload = new JsonCanonicalizer(json.writeValueAsString(data)).getEncodedUTF8();
            if (canonicalPayload.length > maxPayloadBytes) {
                throw new IllegalArgumentException("Event payload is too large");
            }
            Instant normalizedTime = occurredAt == null ? null : occurredAt.truncatedTo(ChronoUnit.MICROS);
            ObjectNode request = json.createObjectNode();
            request.put("event_type", eventType);
            request.put("schema_version", resolvedVersion);
            request.set("data", data);
            if (normalizedTime != null) {
                request.put("occurred_at", normalizedTime.toString());
            }
            byte[] canonicalRequest = new JsonCanonicalizer(json.writeValueAsString(request)).getEncodedUTF8();
            return new EventPayload(eventType, resolvedVersion, normalizedTime,
                    new String(canonicalPayload, StandardCharsets.UTF_8),
                    sha256(canonicalPayload), sha256(canonicalRequest));
        } catch (IOException invalid) {
            throw new IllegalArgumentException("Invalid event", invalid);
        }
    }

    private static void validateJson(JsonNode node, int depth) {
        if (depth > 32) {
            throw new IllegalArgumentException("Event nesting is too deep");
        }
        if (node.isNumber()) {
            if (node.isIntegralNumber() && node.bigIntegerValue().abs().compareTo(MAX_EXACT_INTEGER) > 0
                    || !node.isIntegralNumber() && !Double.isFinite(node.doubleValue())) {
                throw new IllegalArgumentException("Invalid event number");
            }
        }
        if (node.isTextual()) {
            validateUnicode(node.textValue());
        }
        if (node.isObject()) {
            var names = node.fieldNames();
            while (names.hasNext()) {
                validateUnicode(names.next());
            }
        }
        for (JsonNode child : node) {
            validateJson(child, depth + 1);
        }
    }

    private static void validateUnicode(String value) {
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (Character.isHighSurrogate(character)) {
                if (index + 1 >= value.length() || !Character.isLowSurrogate(value.charAt(index + 1))) {
                    throw new IllegalArgumentException("Invalid event Unicode");
                }
                index++;
            } else if (Character.isLowSurrogate(character)) {
                throw new IllegalArgumentException("Invalid event Unicode");
            }
        }
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("SHA-256 is unavailable", unavailable);
        }
    }

    public String eventType() {
        return eventType;
    }

    public int schemaVersion() {
        return schemaVersion;
    }

    public Instant occurredAt() {
        return occurredAt;
    }

    public String payloadJson() {
        return payloadJson;
    }

    public byte[] payloadHash() {
        return payloadHash.clone();
    }

    public byte[] requestHash() {
        return requestHash.clone();
    }

    public boolean matches(byte[] expected) {
        return Arrays.equals(requestHash, expected);
    }
}

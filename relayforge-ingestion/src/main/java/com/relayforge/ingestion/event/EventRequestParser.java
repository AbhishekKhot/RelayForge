package com.relayforge.ingestion.event;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.Set;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public final class EventRequestParser {
    private static final Set<String> FIELDS = Set.of("event_type", "schema_version", "occurred_at", "data");
    private final ObjectMapper strictJson;

    public EventRequestParser() {
        JsonFactory factory = JsonFactory.builder()
                .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                .streamReadConstraints(StreamReadConstraints.builder()
                        .maxNestingDepth(34).maxNumberLength(128).maxStringLength(262144).build())
                .build();
        this.strictJson = new ObjectMapper(factory);
    }

    public ParsedEvent parse(InputStream stream, int maxBytes) {
        if (stream == null || maxBytes < 1) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        }
        byte[] body;
        try {
            body = stream.readNBytes(maxBytes + 1);
        } catch (IOException unavailable) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        }
        if (body.length > maxBytes) {
            throw new ResponseStatusException(HttpStatus.PAYLOAD_TOO_LARGE);
        }
        try {
            JsonNode request;
            try (JsonParser tokens = strictJson.getFactory().createParser(body)) {
                request = strictJson.readTree(tokens);
                if (tokens.nextToken() != null) {
                    throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
                }
            }
            Set<String> names = new HashSet<>();
            if (request != null && request.isObject()) {
                request.fieldNames().forEachRemaining(names::add);
            }
            if (request == null || !request.isObject() || request.size() < 2
                    || !FIELDS.containsAll(names)) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
            }
            JsonNode type = request.get("event_type");
            JsonNode data = request.get("data");
            JsonNode version = request.get("schema_version");
            JsonNode occurred = request.get("occurred_at");
            if (type == null || !type.isTextual() || data == null || !data.isObject()
                    || version != null && !version.isInt()
                    || occurred != null && !occurred.isTextual()) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
            }
            Instant timestamp = occurred == null ? null : Instant.parse(occurred.asText());
            return new ParsedEvent(type.asText(), version == null ? null : version.intValue(),
                    timestamp, data);
        } catch (IOException | DateTimeParseException malformed) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        }
    }

    public record ParsedEvent(String eventType, Integer schemaVersion, Instant occurredAt, JsonNode data) {
    }
}

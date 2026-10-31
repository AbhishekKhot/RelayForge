package com.relayforge.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class OutboxEnvelope {
    private static final int MAX_PAYLOAD_BYTES = 65536;
    private final ObjectMapper json;

    public OutboxEnvelope(ObjectMapper json) {
        this.json = Objects.requireNonNull(json);
    }

    public String encode(ClaimedOutboxRow row) {
        if (row == null || row.id() == null || row.organizationId() == null
                || row.aggregateId() == null || row.aggregateId().isBlank()
                || row.createdAt() == null || row.messageType() == null
                || row.payloadJson() == null) {
            throw invalid();
        }
        int separator = row.messageType().lastIndexOf(".v");
        if (separator < 1 || separator + 2 >= row.messageType().length()) {
            throw invalid();
        }
        int version;
        try {
            version = Integer.parseInt(row.messageType().substring(separator + 2));
        } catch (NumberFormatException malformed) {
            throw invalid();
        }
        if (version < 1 || version > 100 || row.payloadJson().getBytes(StandardCharsets.UTF_8).length
                > MAX_PAYLOAD_BYTES) {
            throw invalid();
        }
        try {
            JsonNode payload = json.readTree(row.payloadJson());
            if (payload == null || !payload.isObject()) {
                throw invalid();
            }
            String messageId = "msg_" + row.id();
            Map<String, Object> envelope = new LinkedHashMap<>();
            envelope.put("message_id", messageId);
            envelope.put("message_type", row.messageType());
            envelope.put("schema_version", version);
            envelope.put("occurred_at", row.createdAt().toString());
            envelope.put("organization_id", row.organizationId().toString());
            envelope.put("aggregate_id", row.aggregateId());
            envelope.put("correlation_id", messageId);
            envelope.put("payload", payload);
            return json.writeValueAsString(envelope);
        } catch (JsonProcessingException malformed) {
            throw invalid();
        }
    }

    private IllegalArgumentException invalid() {
        return new IllegalArgumentException("Invalid outbox envelope");
    }
}

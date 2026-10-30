package com.relayforge.ingestion.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventPayloadTest {
    private final ObjectMapper json = new ObjectMapper();

    @Test
    void hashesEquivalentJsonObjectsIdentically() throws Exception {
        EventPayload first = EventPayload.validate(json, "payment.completed", 1,
                Instant.parse("2026-10-29T10:00:00Z"), json.readTree("{\"b\":2,\"a\":1}"), 262144);
        EventPayload second = EventPayload.validate(json, "payment.completed", 1,
                Instant.parse("2026-10-29T10:00:00Z"), json.readTree("{\"a\":1,\"b\":2}"), 262144);
        assertThat(first.requestHash()).containsExactly(second.requestHash());
        assertThat(first.payloadHash()).containsExactly(second.payloadHash());
    }

    @Test
    void matchesDocumentedCanonicalRequestVectorAndNormalizesTimestamp() throws Exception {
        EventPayload vector = EventPayload.validate(json, "order.created", null, null,
                json.readTree("{\"b\":2,\"a\":1}"), 262144);
        assertThat(HexFormat.of().formatHex(vector.requestHash()))
                .isEqualTo("8b3bea11df1868a5726ddca10480859e250e30242b8a0b867c87bea11453e21d");
        EventPayload offset = EventPayload.validate(json, "order.created", 1,
                Instant.parse("2026-10-29T10:00:00.123456789Z"), json.readTree("{}"), 262144);
        EventPayload truncated = EventPayload.validate(json, "order.created", 1,
                Instant.parse("2026-10-29T10:00:00.123456Z"), json.readTree("{}"), 262144);
        assertThat(offset.requestHash()).containsExactly(truncated.requestHash());
    }

    @Test
    void rejectsUnsafeShapeAndDepth() throws Exception {
        assertThatThrownBy(() -> EventPayload.validate(json, "payment.completed", 1, null,
                json.readTree("{\"x\":9007199254740992}"), 262144))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EventPayload.validate(json, "payment.completed", 1, null,
                json.readTree("{\"x\":1e400}"), 262144))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EventPayload.validate(json, "bad type", 1, null,
                json.readTree("{}"), 262144)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EventPayload.validate(json, "payment.completed", 0, null,
                json.readTree("{}"), 262144)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EventPayload.validate(json, "payment.completed", 1, null,
                json.readTree("[]"), 262144)).isInstanceOf(IllegalArgumentException.class);
        String deep = "[".repeat(33) + "0" + "]".repeat(33);
        assertThatThrownBy(() -> EventPayload.validate(json, "payment.completed", 1, null,
                json.readTree("{\"deep\":" + deep + "}"), 262144))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EventPayload.validate(json, "payment.completed", 1, null,
                json.readTree("{\"large\":\"" + "x".repeat(100) + "\"}"), 80))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

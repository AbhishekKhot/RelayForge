package com.relayforge.outbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutboxEnvelopeTest {
    private final ObjectMapper json = new ObjectMapper();
    private final OutboxEnvelope envelope = new OutboxEnvelope(json);

    @Test
    void encodesStableVersionedEnvelopeAcrossLeaseAttempts() throws Exception {
        UUID id = UUID.randomUUID();
        UUID organization = UUID.randomUUID();
        ClaimedOutboxRow first = row(id, organization, "{\"event_id\":\"evt_123\"}", UUID.randomUUID(), 1);
        ClaimedOutboxRow replay = row(id, organization, "{\"event_id\":\"evt_123\"}", UUID.randomUUID(), 2);

        String encoded = envelope.encode(first);
        assertThat(envelope.encode(replay)).isEqualTo(encoded);
        JsonNode parsed = json.readTree(encoded);
        assertThat(parsed.path("message_id").asText()).isEqualTo("msg_" + id);
        assertThat(parsed.path("message_type").asText()).isEqualTo("event.accepted.v1");
        assertThat(parsed.path("schema_version").asInt()).isEqualTo(1);
        assertThat(parsed.path("organization_id").asText()).isEqualTo(organization.toString());
        assertThat(parsed.path("aggregate_id").asText()).isEqualTo("evt_123");
        assertThat(parsed.path("correlation_id").asText()).isEqualTo("msg_" + id);
        assertThat(parsed.path("occurred_at").asText()).isEqualTo("2026-10-31T00:00:00Z");
        assertThat(parsed.path("payload").path("event_id").asText()).isEqualTo("evt_123");
        assertThat(encoded).doesNotContain("claim_token", "publish_attempts");
    }

    @Test
    void rejectsMalformedOrOversizedPayloadBeforeTransport() {
        UUID id = UUID.randomUUID();
        UUID organization = UUID.randomUUID();
        assertThatThrownBy(() -> envelope.encode(row(id, organization, "[1]", UUID.randomUUID(), 1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> envelope.encode(row(id, organization, "{" + "x".repeat(65537) + "}",
                UUID.randomUUID(), 1))).isInstanceOf(IllegalArgumentException.class);
    }

    private ClaimedOutboxRow row(UUID id, UUID organization, String payload, UUID token, int attempts) {
        return new ClaimedOutboxRow(id, organization, "evt_123", "event.accepted.v1",
                "relayforge.event.accepted.v1", "evt_123", payload, token, attempts,
                Instant.parse("2026-10-31T00:00:00Z"));
    }
}

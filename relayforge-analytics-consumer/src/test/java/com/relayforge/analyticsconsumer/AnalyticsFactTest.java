package com.relayforge.analyticsconsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.contracts.ValidatedEnvelope;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class AnalyticsFactTest {
    private static final UUID ORGANIZATION = UUID.randomUUID();
    private static final UUID FACT = UUID.randomUUID();
    private final ObjectMapper json = new ObjectMapper();

    @Test
    void parsesSanitizedFactAndWritesExplicitNullCorrections() throws Exception {
        AnalyticsFact fact = AnalyticsFact.parse(envelope(payload(
                "\"http_status\":null,\"error_code\":\"LEASE_EXPIRED\",")));
        JsonNode row = json.readTree(fact.json(json));
        assertThat(row.path("fact_id").asText()).isEqualTo(FACT.toString());
        assertThat(row.path("http_status").isNull()).isTrue();
        assertThat(row.path("occurred_at").asText()).isEqualTo("2026-11-03 23:30:00.123");
        assertThat(row.path("error_code").asText()).isEqualTo("LEASE_EXPIRED");
    }

    @Test
    void rejectsSecretFieldsWrongUtcDateAndInvalidVersion() throws Exception {
        assertThatThrownBy(() -> AnalyticsFact.parse(envelope(payload(
                "\"client_secret\":\"never-store\","))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AnalyticsFact.parse(envelope(payload("", "2026-11-04", 1))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AnalyticsFact.parse(envelope(payload("", "2026-11-03", 0))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private ValidatedEnvelope envelope(String body) throws Exception {
        return new ValidatedEnvelope("msg_" + UUID.randomUUID(), "analytics.delivery-facts.v1",
                1, Instant.parse("2026-11-03T23:30:00.123Z"), ORGANIZATION,
                FACT.toString(), "correlation", json.readTree(body));
    }

    private String payload(String extra) {
        return payload(extra, "2026-11-03", 1);
    }

    private String payload(String extra, String date, int version) {
        return "{\"organization_id\":\"" + ORGANIZATION + "\",\"fact_id\":\""
                + FACT + "\",\"event_date\":\"" + date + "\",\"version\":" + version
                + ",\"fact_type\":\"delivery.attempt.finished\",\"status\":\"UNKNOWN\","
                + extra + "\"occurred_at\":\"2026-11-03T23:30:00.123Z\"}";
    }
}

package com.relayforge.ingestion.event;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventRequestParserTest {
    private final EventRequestParser parser = new EventRequestParser();

    @Test
    void rejectsDuplicateFields() {
        assertBadRequest("{\"event_type\":\"a\",\"event_type\":\"b\",\"schema_version\":1,\"data\":{}}");
        assertBadRequest("{\"event_type\":\"a\",\"schema_version\":1,\"data\":{\"x\":1,\"x\":2}}");
    }

    @Test
    void capsRawBodyBeforeParsing() {
        String body = "{\"event_type\":\"a\",\"schema_version\":1,\"data\":{\"x\":\""
                + "x".repeat(100) + "\"}}";
        assertThatThrownBy(() -> parse(body, 80))
                .isInstanceOf(ResponseStatusException.class)
                .satisfies(error -> assertThat(((ResponseStatusException) error).getStatusCode())
                        .isEqualTo(HttpStatus.PAYLOAD_TOO_LARGE));
    }

    @Test
    void rejectsDeepJsonDuringParsing() {
        String body = "{\"event_type\":\"a\",\"data\":{\"x\":"
                + "[".repeat(100) + "0" + "]".repeat(100) + "}}";
        assertBadRequest(body);
    }

    @Test
    void parsesValidOptionalTimestampAndRejectsExtraFields() {
        var parsed = parse("{\"event_type\":\"payment.completed\",\"schema_version\":1,"
                + "\"occurred_at\":\"2026-10-29T10:00:00Z\",\"data\":{\"x\":1}}", 1024);
        assertThat(parsed.occurredAt().toString()).isEqualTo("2026-10-29T10:00:00Z");
        assertBadRequest("{\"event_type\":\"a\",\"schema_version\":1,\"data\":{},\"unknown\":1}");
        assertBadRequest("{\"event_type\":\"a\",\"data\":{}} {\"extra\":true}");
    }

    private void assertBadRequest(String body) {
        assertThatThrownBy(() -> parse(body, 1024))
                .isInstanceOf(ResponseStatusException.class)
                .satisfies(error -> assertThat(((ResponseStatusException) error).getStatusCode())
                        .isEqualTo(HttpStatus.BAD_REQUEST));
    }

    private EventRequestParser.ParsedEvent parse(String body, int limit) {
        return parser.parse(new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)), limit);
    }
}

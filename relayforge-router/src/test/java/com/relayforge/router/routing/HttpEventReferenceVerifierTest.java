package com.relayforge.router.routing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.contracts.ValidatedEnvelope;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class HttpEventReferenceVerifierTest {
    private static final UUID ORGANIZATION = UUID.fromString("c35cc995-96db-42b5-b11a-27070842d274");
    private static final UUID EVENT = UUID.fromString("0e2b6761-d796-465d-8e54-92a6bef5cd97");
    private static final String LOCATOR = "evt_testLocator";
    private static final String HASH = "a".repeat(64);
    private static final String CREDENTIAL = "test-router-credential-01234567890123456789";
    private final ObjectMapper json = new ObjectMapper();

    @Test
    void verifiesEventMetadataAndSendsOnlyDedicatedCredential() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        HttpServer server = server((exchange) -> {
            assertThat(exchange.getRequestHeaders().getFirst("X-RelayForge-Router-Event-Credential"))
                    .isEqualTo(CREDENTIAL);
            assertThat(exchange.getRequestURI().getRawQuery()).contains(ORGANIZATION.toString());
            calls.incrementAndGet();
            respond(exchange, 200, response("order.created"));
        });
        try {
            HttpEventReferenceVerifier verifier = verifier(server);
            VerifiedEvent verified = verifier.verify(envelope());
            assertThat(verified.eventId()).isEqualTo(EVENT);
            assertThat(verified.organizationId()).isEqualTo(ORGANIZATION);
            assertThat(calls).hasValue(1);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void rejectsMismatchedAuthoritativeEvent() throws Exception {
        HttpServer server = server(exchange -> respond(exchange, 200, response("other.type")));
        try {
            assertThatThrownBy(() -> verifier(server).verify(envelope()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageNotContaining(CREDENTIAL);
        } finally {
            server.stop(0);
        }
    }

    private HttpEventReferenceVerifier verifier(HttpServer server) {
        return new HttpEventReferenceVerifier(URI.create("http://127.0.0.1:"
                + server.getAddress().getPort()), CREDENTIAL, json, "dlv-a");
    }

    private ValidatedEnvelope envelope() throws Exception {
        var payload = json.readTree("""
                {"organization_id":"%s","event_id":"%s","accepted_day":"2026-11-02",
                 "accepted_at":"2026-11-02T10:00:00Z","event_type":"order.created",
                 "payload_hash":"%s","ingestion_shard_id":"ing-a","delivery_shard_id":"dlv-a"}
                """.formatted(ORGANIZATION, LOCATOR, HASH));
        return new ValidatedEnvelope("msg_1", "event.accepted.v1", 1,
                Instant.parse("2026-11-02T10:00:00Z"), ORGANIZATION,
                EVENT.toString(), "msg_1", payload);
    }

    private String response(String type) {
        return """
                {"organization_id":"%s","event_id":"%s","accepted_day":"2026-11-02",
                 "accepted_at":"2026-11-02T10:00:00Z","event_type":"%s",
                 "payload_hash":"%s"}
                """.formatted(ORGANIZATION, EVENT, type, HASH);
    }

    private HttpServer server(Handler handler) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/internal/v1/events/", exchange -> handler.handle(exchange));
        server.start();
        return server;
    }

    private void respond(com.sun.net.httpserver.HttpExchange exchange, int status, String body)
            throws java.io.IOException {
        byte[] bytes = body.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    @FunctionalInterface
    private interface Handler {
        void handle(com.sun.net.httpserver.HttpExchange exchange) throws java.io.IOException;
    }
}

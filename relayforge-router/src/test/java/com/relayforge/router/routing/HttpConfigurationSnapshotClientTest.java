package com.relayforge.router.routing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class HttpConfigurationSnapshotClientTest {
    private static final String CREDENTIAL = "test-config-credential-01234567890123456789";
    private static final UUID ORGANIZATION = UUID.fromString("8420e2e8-f544-4a4b-aa92-9e1655d6f43c");
    private static final UUID ENDPOINT = UUID.fromString("7c7eac67-2741-4e37-9a26-75f13f1feec8");
    private static final UUID SUBSCRIPTION = UUID.fromString("3d22dbb7-9b3d-4575-b4be-811ca12f97f1");

    @Test
    void loadsSecretFreePagesAtOneRevision() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        HttpServer server = server(exchange -> {
            assertThat(exchange.getRequestHeaders().getFirst("X-RelayForge-Router-Config-Credential"))
                    .isEqualTo(CREDENTIAL);
            calls.incrementAndGet();
            String body;
            if (exchange.getRequestURI().getPath().endsWith("/subscriptions")) {
                body = """
                        {"source_revision":4,"data":[{"subscription_id":"%s","endpoint_id":"%s",
                         "event_type":"order.created","status":"ACTIVE","version":2,
                         "filter_config":null}],"next_cursor":null}
                        """.formatted(SUBSCRIPTION, ENDPOINT);
            } else {
                body = """
                        {"source_revision":4,"data":[{"endpoint_id":"%s","status":"ACTIVE",
                         "version":2}],"next_cursor":null}
                        """.formatted(ENDPOINT);
            }
            respond(exchange, body);
        });
        try {
            ConfigurationSnapshot snapshot = client(server).load(ORGANIZATION);
            assertThat(snapshot.revision()).isEqualTo(4);
            assertThat(snapshot.endpoints()).hasSize(1);
            assertThat(snapshot.subscriptions()).hasSize(1);
            assertThat(calls).hasValueGreaterThanOrEqualTo(3);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void rejectsChangingRevisionAcrossPages() throws Exception {
        HttpServer server = server(exchange -> respond(exchange,
                exchange.getRequestURI().getPath().endsWith("/subscriptions")
                        ? "{\"source_revision\":5,\"data\":[],\"next_cursor\":null}"
                        : "{\"source_revision\":4,\"data\":[],\"next_cursor\":null}"));
        try {
            assertThatThrownBy(() -> client(server).load(ORGANIZATION))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageNotContaining(CREDENTIAL);
        } finally {
            server.stop(0);
        }
    }

    private HttpConfigurationSnapshotClient client(HttpServer server) {
        return new HttpConfigurationSnapshotClient(URI.create("http://127.0.0.1:"
                + server.getAddress().getPort()), CREDENTIAL, new ObjectMapper());
    }

    private HttpServer server(Handler handler) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/internal/v1/organizations/", exchange -> handler.handle(exchange));
        server.start();
        return server;
    }

    private void respond(com.sun.net.httpserver.HttpExchange exchange, String body)
            throws java.io.IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    @FunctionalInterface
    private interface Handler {
        void handle(com.sun.net.httpserver.HttpExchange exchange) throws java.io.IOException;
    }
}

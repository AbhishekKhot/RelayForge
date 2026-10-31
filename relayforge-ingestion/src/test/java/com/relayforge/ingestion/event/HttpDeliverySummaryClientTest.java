package com.relayforge.ingestion.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HttpDeliverySummaryClientTest {
    private static final String CREDENTIAL = "test-summary-credential-01234567890123456789";
    private HttpServer server;
    private volatile String body;
    private volatile int status;
    private volatile long responseDelayMillis;

    @BeforeEach
    void start() throws Exception {
        body = "{\"status\":\"PENDING\",\"target_count\":0,\"delivery_count\":0,"
                + "\"succeeded_count\":0,\"dead_lettered_count\":0}";
        status = 200;
        responseDelayMillis = 0;
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/internal/v1/events", exchange -> {
            if (responseDelayMillis > 0) {
                try {
                    Thread.sleep(responseDelayMillis);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    @Test
    void readsPendingWithoutInventingDeliveryCounts() {
        DeliverySummary summary = client().read(UUID.randomUUID(), java.time.LocalDate.now(), UUID.randomUUID());
        assertThat(summary.status()).isEqualTo("PENDING");
        assertThat(summary.deliveryCount()).isZero();
    }

    @Test
    void rejectsUnavailableOversizedAndMalformedResponses() {
        status = 503;
        assertUnavailable();
        status = 200;
        body = "x".repeat(4097);
        assertUnavailable();
        body = "{\"status\":\"ROUTED\",\"target_count\":1,\"delivery_count\":\"1\","
                + "\"succeeded_count\":0,\"dead_lettered_count\":0}";
        assertUnavailable();
    }

    @Test
    void timesOutAnUnresponsiveDeliveryService() {
        responseDelayMillis = 4000;
        assertUnavailable();
    }

    private void assertUnavailable() {
        assertThatThrownBy(() -> client().read(UUID.randomUUID(), java.time.LocalDate.now(), UUID.randomUUID()))
                .isInstanceOf(ResponseStatusException.class)
                .satisfies(failure -> assertThat(((ResponseStatusException) failure).getStatusCode())
                        .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE));
    }

    private HttpDeliverySummaryClient client() {
        return new HttpDeliverySummaryClient(
                java.net.URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
                CREDENTIAL, new ObjectMapper());
    }
}

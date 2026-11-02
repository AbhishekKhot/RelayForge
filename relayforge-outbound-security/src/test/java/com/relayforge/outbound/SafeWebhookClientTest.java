package com.relayforge.outbound;

import com.sun.net.httpserver.HttpServer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SafeWebhookClientTest {
    @Test
    void boundsResponseExcerptAndOverallDeadline() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/large", exchange -> {
            byte[] body = "x".repeat(10000).getBytes();
            exchange.sendResponseHeaders(500, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.createContext("/slow", exchange -> {
            try {
                Thread.sleep(2000);
                exchange.sendResponseHeaders(200, -1);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } finally {
                exchange.close();
            }
        });
        server.start();
        try {
            URI simulator = URI.create("http://localhost:" + server.getAddress().getPort());
            DestinationPolicy policy = new DestinationPolicy(
                    host -> new InetAddress[]{InetAddress.getByName("127.0.0.1")}, true, simulator);
            try (SafeWebhookClient client = new SafeWebhookClient(policy)) {
                var large = policy.validate(new Destination(simulator.toString(), "/large",
                        Map.of(), Map.of()));
                assertThat(client.send(large, new byte[0], Duration.ofSeconds(2), Map.of(),
                        () -> true).excerpt()).hasSize(1024);
                var slow = policy.validate(new Destination(simulator.toString(), "/slow",
                        Map.of(), Map.of()));
                Instant start = Instant.now();
                assertThatThrownBy(() -> client.send(slow, new byte[0], Duration.ofMillis(200),
                        Map.of(), () -> true)).isInstanceOf(java.io.IOException.class);
                assertThat(Duration.between(start, Instant.now())).isLessThan(Duration.ofSeconds(1));
            }
        } finally {
            server.stop(0);
        }
    }

    @Test
    void sendsOnlyWorkerAuthorizedReservedHeaders() throws Exception {
        AtomicReference<String> received = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/hook", exchange -> {
            received.set(exchange.getRequestHeaders().getFirst("X-RelayForge-Signature"));
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
        });
        server.start();
        try {
            URI simulator = URI.create("http://localhost:" + server.getAddress().getPort());
            DestinationPolicy policy = new DestinationPolicy(
                    host -> new InetAddress[]{InetAddress.getByName("127.0.0.1")}, true, simulator);
            ValidatedDestination destination = policy.validate(
                    new Destination(simulator.toString(), "/hook", Map.of(), Map.of()));
            try (SafeWebhookClient client = new SafeWebhookClient(policy)) {
                assertThat(client.send(destination, "{}".getBytes(), Duration.ofSeconds(2),
                        Map.of("X-RelayForge-Signature", "v1=test"))).isEqualTo(204);
                assertThatThrownBy(() -> client.send(destination, "{}".getBytes(), Duration.ofSeconds(2),
                        Map.of("Host", "private.example"))).isInstanceOf(IllegalArgumentException.class);
            }
            assertThat(received).hasValue("v1=test");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void doesNotFollowRedirectToAnotherDestination() throws Exception {
        AtomicInteger targetCalls = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/redirect", exchange -> {
            exchange.getResponseHeaders().add("Location", "/target");
            exchange.sendResponseHeaders(302, -1);
            exchange.close();
        });
        server.createContext("/target", exchange -> {
            targetCalls.incrementAndGet();
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.start();
        try {
            URI simulator = URI.create("http://localhost:" + server.getAddress().getPort());
            DestinationPolicy policy = new DestinationPolicy(
                    host -> new InetAddress[]{InetAddress.getByName("127.0.0.1")}, true, simulator);
            ValidatedDestination destination = policy.validate(
                    new Destination(simulator.toString(), "/redirect", Map.of(), Map.of()));
            try (SafeWebhookClient client = new SafeWebhookClient(policy)) {
                assertThat(client.send(destination, new byte[0], Duration.ofSeconds(2))).isEqualTo(302);
            }
            assertThat(targetCalls).hasValue(0);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void rejectsDnsRebindingAtConnectionTime() throws Exception {
        AtomicInteger resolutions = new AtomicInteger();
        DestinationPolicy policy = new DestinationPolicy(host -> new InetAddress[]{
                InetAddress.getByName(resolutions.incrementAndGet() == 1 ? "8.8.8.8" : "127.0.0.1")},
                false, null);
        ValidatedDestination destination = policy.validate(new Destination(
                "https://public.example", "/hook", Map.of(), Map.of()));

        try (SafeWebhookClient client = new SafeWebhookClient(policy)) {
            assertThatThrownBy(() -> client.send(destination, new byte[0], Duration.ofSeconds(2)))
                    .isInstanceOf(UnsafeDestinationException.class);
        }
        assertThat(resolutions).hasValue(2);
    }
}

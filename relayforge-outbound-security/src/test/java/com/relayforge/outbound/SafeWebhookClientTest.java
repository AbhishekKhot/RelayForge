package com.relayforge.outbound;

import com.sun.net.httpserver.HttpServer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SafeWebhookClientTest {
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

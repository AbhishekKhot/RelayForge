package com.relayforge.deliveryworker.http;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class DeliveryReferenceClientTest {
    @Test
    void distinguishesInactiveEndpointFromUnavailableControlPlane() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        AtomicInteger status = new AtomicInteger(404);
        server.createContext("/internal/v1/organizations", exchange -> {
            exchange.sendResponseHeaders(status.get(), -1);
            exchange.close();
        });
        server.start();
        try {
            URI base = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
            var client = new DeliveryReferenceClient(base, base, "x".repeat(32),
                    "y".repeat(32), new ObjectMapper());
            assertThatThrownBy(() -> client.endpoint(UUID.randomUUID(), UUID.randomUUID()))
                    .isInstanceOf(DeliveryReferenceClient.EndpointNotActiveException.class);
            status.set(503);
            assertThatThrownBy(() -> client.endpoint(UUID.randomUUID(), UUID.randomUUID()))
                    .isInstanceOf(java.io.IOException.class)
                    .isNotInstanceOf(DeliveryReferenceClient.EndpointNotActiveException.class);
        } finally {
            server.stop(0);
        }
    }
}

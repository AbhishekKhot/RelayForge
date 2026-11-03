package com.relayforge.analyticsapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

class AnalyticsAuthenticatorTest {
    @Test
    void checksAnalyticsScopeAndUsesAuthenticatedOrganization() throws Exception {
        UUID organization = UUID.randomUUID();
        AtomicReference<String> scope = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/auth/check", exchange -> {
            scope.set(exchange.getRequestURI().getRawQuery());
            int status = "Bearer permitted".equals(exchange.getRequestHeaders().getFirst("Authorization"))
                    ? 200 : 403;
            byte[] body = ("{\"organization_id\":\"" + organization + "\"}")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        try {
            AnalyticsAuthenticator authenticator = new AnalyticsAuthenticator(URI.create(
                    "http://127.0.0.1:" + server.getAddress().getPort()), new ObjectMapper());
            assertThat(authenticator.authenticate("Bearer permitted")).isEqualTo(organization);
            assertThat(scope).hasValue("scope=analytics:read");
            assertThatThrownBy(() -> authenticator.authenticate("Bearer denied"))
                    .isInstanceOfSatisfying(ResponseStatusException.class, failure ->
                            assertThat(failure.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN));
            assertThatThrownBy(() -> authenticator.authenticate(null))
                    .isInstanceOfSatisfying(ResponseStatusException.class, failure ->
                            assertThat(failure.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED));
        } finally {
            server.stop(0);
        }
    }
}

package com.relayforge.controlplane;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("local")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class EndpointRegistrationIT {
    private static final String PROVISIONING_CREDENTIAL = "trusted-provisioning-credential-for-tests-12345";
    private static final PostgreSQLContainer<?> DATABASE = new PostgreSQLContainer<>("postgres:17.11")
            .withDatabaseName("relayforge_control")
            .withUsername("relayforge")
            .withPassword("relayforge-test");
    private static final HttpServer SIMULATOR;

    static {
        try {
            DATABASE.start();
            SIMULATOR = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            SIMULATOR.createContext("/hook", exchange -> {
                exchange.sendResponseHeaders(204, -1);
                exchange.close();
            });
            SIMULATOR.start();
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }

    @AfterAll
    static void stopSimulator() {
        SIMULATOR.stop(0);
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.flyway.url", DATABASE::getJdbcUrl);
        registry.add("spring.flyway.user", DATABASE::getUsername);
        registry.add("spring.flyway.password", DATABASE::getPassword);
        registry.add("relayforge.database.url", DATABASE::getJdbcUrl);
        registry.add("relayforge.database.username", DATABASE::getUsername);
        registry.add("relayforge.database.password", DATABASE::getPassword);
        registry.add("relayforge.provisioning.credential", () -> PROVISIONING_CREDENTIAL);
        registry.add("relayforge.security.api-key-hmac-key", () -> "0123456789abcdef".repeat(4));
        registry.add("relayforge.ssrf.local-simulator-url",
                () -> "http://localhost:" + SIMULATOR.getAddress().getPort());
    }

    @Autowired
    private TestRestTemplate client;

    @Test
    void registersValidatedSimulatorEndpointAndSubscriptionsAtomically() throws Exception {
        Credentials credentials = credentials();
        ResponseEntity<JsonNode> created = register(credentials.key(), request(simulatorUrl()));

        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String endpointId = created.getBody().path("data").path("endpoint_id").asText();
        assertThat(created.getBody().toString()).doesNotContain(credentials.key());
        assertThat(count("SELECT count(*) FROM endpoints WHERE id = ? AND organization_id = ?",
                UUID.fromString(endpointId), credentials.organizationId())).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM subscriptions WHERE endpoint_id = ? AND organization_id = ?",
                UUID.fromString(endpointId), credentials.organizationId())).isEqualTo(2);
    }

    @Test
    void rejectsMetadataAddressAndDoesNotPersistEndpoint() throws Exception {
        Credentials credentials = credentials();
        ResponseEntity<JsonNode> rejected = register(credentials.key(), request("https://169.254.169.254"));

        assertThat(rejected.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(rejected.getBody().toString()).doesNotContain("169.254.169.254");
        assertThat(count("SELECT count(*) FROM endpoints WHERE organization_id = ?",
                credentials.organizationId())).isZero();
    }

    @Test
    void rejectsMalformedHeadersPolicyAndDuplicateEventTypesBeforeWriting() throws Exception {
        Credentials credentials = credentials();
        Map<String, Object> badHeader = request(simulatorUrl(), Map.of("Host", "internal"),
                List.of("payment.completed", "payment.failed"), 10000);
        Map<String, Object> badTimeout = request(simulatorUrl(), Map.of(),
                List.of("payment.completed", "payment.failed"), 60001);
        Map<String, Object> duplicateEvents = request(simulatorUrl(), Map.of(),
                List.of("payment.completed", "payment.completed"), 10000);
        Map<String, Object> badQuery = new HashMap<>(request(simulatorUrl(), Map.of(),
                List.of("payment.completed"), 10000));
        @SuppressWarnings("unchecked")
        Map<String, Object> destination = new HashMap<>((Map<String, Object>) badQuery.get("destination"));
        destination.put("query_params", Map.of("bad\nname", "value"));
        badQuery.put("destination", destination);

        for (Map<String, Object> body : List.of(badHeader, badTimeout, duplicateEvents, badQuery)) {
            assertThat(register(credentials.key(), body).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        }
        assertThat(count("SELECT count(*) FROM endpoints WHERE organization_id = ?",
                credentials.organizationId())).isZero();
    }

    private Credentials credentials() {
        HttpHeaders admin = new HttpHeaders();
        admin.setContentType(MediaType.APPLICATION_JSON);
        admin.set("X-Provisioning-Credential", PROVISIONING_CREDENTIAL);
        admin.set("Idempotency-Key", UUID.randomUUID().toString());
        ResponseEntity<JsonNode> organization = client.exchange("/v1/organizations", HttpMethod.POST,
                new HttpEntity<>(Map.of("name", "Endpoint Owner"), admin), JsonNode.class);
        assertThat(organization.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String firstKey = organization.getBody().path("data").path("api_key").asText();
        String keyId = firstKey.split("_")[2];
        ResponseEntity<JsonNode> rotated = client.exchange("/v1/api-keys/" + keyId + "/rotate", HttpMethod.POST,
                new HttpEntity<>(Map.of("scopes", List.of("endpoints:write")), admin), JsonNode.class);
        assertThat(rotated.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        return new Credentials(UUID.fromString(organization.getBody().path("data").path("organization_id").asText()),
                rotated.getBody().path("data").path("api_key").asText());
    }

    private ResponseEntity<JsonNode> register(String key, Map<String, Object> body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(key);
        return client.exchange("/v1/endpoints", HttpMethod.POST, new HttpEntity<>(body, headers), JsonNode.class);
    }

    private Map<String, Object> request(String baseUrl) {
        return request(baseUrl, Map.of("Content-Type", "application/json"),
                List.of("payment.completed", "payment.failed"), 10000);
    }

    private Map<String, Object> request(String baseUrl, Map<String, String> headers,
            List<String> eventTypes, int timeoutMs) {
        return Map.of("name", "Payments", "destination", Map.of(
                        "base_url", baseUrl, "path", "/hook", "query_params", Map.of("source", "test"),
                        "headers", headers, "metadata", Map.of("environment", "test")),
                "authentication", Map.of("type", "NONE"),
                "event_types", eventTypes,
                "delivery_policy", Map.of("timeout_ms", timeoutMs, "max_attempts", 3, "max_concurrency", 2));
    }

    private String simulatorUrl() {
        return "http://localhost:" + SIMULATOR.getAddress().getPort();
    }

    private long count(String sql, Object... values) throws Exception {
        try (var connection = DriverManager.getConnection(
                DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword());
                var statement = connection.prepareStatement(sql)) {
            for (int index = 0; index < values.length; index++) {
                statement.setObject(index + 1, values[index]);
            }
            try (var rows = statement.executeQuery()) {
                rows.next();
                return rows.getLong(1);
            }
        }
    }

    private record Credentials(UUID organizationId, String key) {
        @Override
        public String toString() {
            return "Credentials[organizationId=" + organizationId + ", key=[REDACTED]]";
        }
    }
}

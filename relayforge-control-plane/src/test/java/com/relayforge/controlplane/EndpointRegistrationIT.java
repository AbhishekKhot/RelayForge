package com.relayforge.controlplane;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
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
        registry.add("relayforge.encryption.local-keys", () -> "local-v1:" + "fedcba9876543210".repeat(4));
        registry.add("relayforge.encryption.active-version", () -> "local-v1");
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

    @Test
    void encryptsSupportedAuthenticationAndReturnsOnlyRedactedConfiguration() throws Exception {
        Credentials credentials = credentials();
        List<Map<String, Object>> configurations = List.of(
                Map.of("type", "STATIC_HEADER", "header_name", "X-Api-Key", "value", "static-secret"),
                Map.of("type", "STATIC_API_KEY", "header_name", "X-Api-Key", "value", "static-secret"),
                Map.of("type", "BEARER", "token", "bearer-secret"),
                Map.of("type", "BASIC", "username", "customer", "password", "basic-secret"),
                Map.of("type", "CLIENT_CREDENTIALS", "token_url", simulatorUrl() + "/token",
                        "client_id", "client", "client_secret", "oauth-secret",
                        "token_response", Map.of("access_token_path", "$.access_token",
                                "token_type", "Bearer")),
                Map.of("type", "TOKEN_EXCHANGE", "token_request", Map.of(
                                "method", "POST", "url", simulatorUrl() + "/token",
                                "headers", Map.of("Content-Type", "application/json"),
                                "body_fields", Map.of("username", Map.of("source", "secret", "name", "username"))),
                        "token_response", Map.of("access_token_path", "$.data.accessToken",
                                "token_type", "Bearer"),
                        "token_placement", Map.of("type", "HEADER", "name", "Authorization",
                                "prefix", "Bearer "), "secrets", Map.of("username", "exchange-secret")));
        for (Map<String, Object> configuration : configurations) {
            Map<String, Object> body = new HashMap<>(request(simulatorUrl()));
            body.put("authentication", configuration);
            ResponseEntity<JsonNode> created = register(credentials.key(), body);
            assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
            String id = created.getBody().path("data").path("endpoint_id").asText();
            ResponseEntity<JsonNode> read = readEndpoint(credentials.key(), id);
            assertThat(read.getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(read.getBody().path("data").path("authentication").path("type").asText())
                    .isEqualTo(configuration.get("type"));
            assertThat(read.getBody().toString()).doesNotContain("secret", "ciphertext", "wrapped_dek");
            try (var connection = DriverManager.getConnection(
                    DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword());
                    var statement = connection.prepareStatement("""
                            SELECT auth_type, auth_config_ciphertext, auth_wrapped_dek, auth_nonce
                            FROM endpoints WHERE id = ? AND organization_id = ?
                            """)) {
                statement.setObject(1, UUID.fromString(id));
                statement.setObject(2, credentials.organizationId());
                try (var row = statement.executeQuery()) {
                    assertThat(row.next()).isTrue();
                    assertThat(row.getString(1)).isEqualTo(configuration.get("type"));
                    assertThat(row.getBytes(2)).hasSizeGreaterThan(16);
                    assertThat(row.getBytes(3)).hasSizeGreaterThan(16);
                    assertThat(row.getBytes(4)).hasSize(12);
                    assertThat(new String(row.getBytes(2), StandardCharsets.ISO_8859_1))
                            .doesNotContain("secret");
                }
            }
        }
    }

    @Test
    void rejectsConflictingStaticHeaderAndNeverStoresSecret() throws Exception {
        Credentials credentials = credentials();
        Map<String, Object> body = new HashMap<>(request(simulatorUrl(), Map.of("X-Api-Key", "public"),
                List.of("payment.completed"), 10000));
        body.put("authentication", Map.of("type", "STATIC_HEADER", "header_name", "X-Api-Key",
                "value", "private-secret"));

        ResponseEntity<JsonNode> rejected = register(credentials.key(), body);
        assertThat(rejected.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(rejected.getBody().toString()).doesNotContain("private-secret");
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
                new HttpEntity<>(Map.of("scopes", List.of("endpoints:write", "endpoints:read")), admin), JsonNode.class);
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

    private ResponseEntity<JsonNode> readEndpoint(String key, String id) {
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(key);
        return client.exchange("/v1/endpoints/" + id, HttpMethod.GET,
                new HttpEntity<>(headers), JsonNode.class);
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

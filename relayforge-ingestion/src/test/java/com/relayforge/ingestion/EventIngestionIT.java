package com.relayforge.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import com.relayforge.ingestion.event.DurabilityGate;
import com.relayforge.ingestion.event.IngestionRepository;
import java.net.InetSocketAddress;
import java.sql.DriverManager;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class EventIngestionIT {
    private static final UUID ORGANIZATION = UUID.fromString("00000000-0000-0000-0000-000000000001");
    private static final UUID EXPIRY_ORGANIZATION = UUID.fromString("00000000-0000-0000-0000-000000000002");
    private static final UUID LIMITED_ORGANIZATION = UUID.fromString("00000000-0000-0000-0000-000000000003");
    private static final PostgreSQLContainer<?> DATABASE = new PostgreSQLContainer<>("postgres:17.11")
            .withDatabaseName("relayforge_ingestion")
            .withUsername("relayforge")
            .withPassword("relayforge-test");
    private static final HttpServer AUTH;

    static {
        try {
            DATABASE.start();
            AUTH = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            AUTH.createContext("/v1/auth/check", exchange -> {
                String authorization = exchange.getRequestHeaders().getFirst("Authorization");
                boolean accepted = "Bearer good-key".equals(authorization)
                        || "Bearer expiry-key".equals(authorization)
                        || "Bearer limited-key".equals(authorization);
                UUID owner = "Bearer expiry-key".equals(authorization) ? EXPIRY_ORGANIZATION
                        : "Bearer limited-key".equals(authorization) ? LIMITED_ORGANIZATION : ORGANIZATION;
                byte[] body = (accepted ? "{\"organization_id\":\"" + owner
                        + "\",\"events_per_minute\":"
                        + (owner.equals(LIMITED_ORGANIZATION) ? 1 : 1200) + "}" : "{}").getBytes();
                exchange.sendResponseHeaders(accepted ? 200 : 401, body.length);
                exchange.getResponseBody().write(body);
                exchange.close();
            });
            AUTH.start();
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.flyway.url", DATABASE::getJdbcUrl);
        registry.add("spring.flyway.user", DATABASE::getUsername);
        registry.add("spring.flyway.password", DATABASE::getPassword);
        registry.add("relayforge.database.url", DATABASE::getJdbcUrl);
        registry.add("relayforge.database.username", DATABASE::getUsername);
        registry.add("relayforge.database.password", DATABASE::getPassword);
        registry.add("relayforge.control.internal-url", () -> "http://127.0.0.1:" + AUTH.getAddress().getPort());
        registry.add("relayforge.ingestion.acceptance-enabled", () -> "true");
    }

    @TestConfiguration
    static class DurabilityForTransactionTests {
        @Bean
        @Primary
        DurabilityGate durableForTest() {
            return () -> { };
        }
    }

    @Autowired
    private TestRestTemplate client;

    @Autowired
    private IngestionRepository repository;

    @Test
    void acceptsOnceAndWritesEventAndOutboxAtomically() throws Exception {
        String key = "payment-123-completed";
        ResponseEntity<JsonNode> first = publish("good-key", key, "payment.completed", Map.of("amount", 1500));
        ResponseEntity<JsonNode> repeat = publish("good-key", key, "payment.completed", Map.of("amount", 1500));
        ResponseEntity<JsonNode> conflict = publish("good-key", key, "payment.completed", Map.of("amount", 2000));

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(repeat.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(repeat.getBody().path("data").path("event_id"))
                .isEqualTo(first.getBody().path("data").path("event_id"));
        assertThat(conflict.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(count("SELECT count(*) FROM events WHERE organization_id = ?", ORGANIZATION)).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM ingestion_outbox WHERE organization_id = ?", ORGANIZATION)).isEqualTo(1);
    }

    @Test
    void rejectsUnauthenticatedAndStaleFenceBeforeWriting() throws Exception {
        assertThat(publish("wrong-key", "unauthorized-1", "payment.completed", Map.of()).getStatusCode())
                .isEqualTo(HttpStatus.UNAUTHORIZED);
        try (var connection = DriverManager.getConnection(
                DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword());
                var statement = connection.createStatement()) {
            statement.executeUpdate("UPDATE bucket_fence SET epoch = 2 WHERE bucket_id = 3410");
        }
        assertThat(publish("good-key", "order-123", "payment.completed", Map.of()).getStatusCode())
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    }

    @Test
    void reusesKeyOnlyAfterNinetyDayRegistryExpiry() throws Exception {
        String key = "expiring-key";
        ResponseEntity<JsonNode> first = publish("expiry-key", key, "payment.completed", Map.of());
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        try (var connection = DriverManager.getConnection(
                DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword());
                var statement = connection.prepareStatement("""
                        WITH aged AS (SELECT clock_timestamp() - interval '91 days' AS accepted)
                        UPDATE idempotency_registry SET accepted_at = aged.accepted,
                            expires_at = aged.accepted + interval '2160 hours',
                            accepted_day = (aged.accepted AT TIME ZONE 'UTC')::date
                        FROM aged WHERE organization_id = ? AND idempotency_key = ?
                        """)) {
            statement.setObject(1, EXPIRY_ORGANIZATION);
            statement.setString(2, key);
            assertThat(statement.executeUpdate()).isEqualTo(1);
        }
        ResponseEntity<JsonNode> second = publish("expiry-key", key, "payment.completed", Map.of());
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(second.getBody().path("data").path("event_id"))
                .isNotEqualTo(first.getBody().path("data").path("event_id"));
        assertThat(count("SELECT count(*) FROM events WHERE organization_id = ?", EXPIRY_ORGANIZATION))
                .isEqualTo(2);
    }

    @Test
    void rejectsOversizedAndDuplicatePropertyRequestsBeforeWriting() throws Exception {
        long before = count("SELECT count(*) FROM events WHERE organization_id = ?", ORGANIZATION);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth("good-key");
        headers.set("Idempotency-Key", "invalid-request-" + UUID.randomUUID());
        String oversized = "{\"event_type\":\"payment.completed\",\"data\":{\"x\":\""
                + "x".repeat(262144) + "\"}}";
        ResponseEntity<JsonNode> large = client.exchange("/v1/events", HttpMethod.POST,
                new HttpEntity<>(oversized, headers), JsonNode.class);
        assertThat(large.getStatusCode()).isEqualTo(HttpStatus.PAYLOAD_TOO_LARGE);
        String duplicate = "{\"event_type\":\"payment.completed\",\"data\":{\"x\":1,\"x\":2}}";
        ResponseEntity<JsonNode> malformed = client.exchange("/v1/events", HttpMethod.POST,
                new HttpEntity<>(duplicate, headers), JsonNode.class);
        assertThat(malformed.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(count("SELECT count(*) FROM events WHERE organization_id = ?", ORGANIZATION)).isEqualTo(before);
    }

    @Test
    void removesOnlyExpiredRateCountersInBoundedBatches() throws Exception {
        UUID owner = UUID.randomUUID();
        try (var connection = DriverManager.getConnection(
                DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword());
                var statement = connection.prepareStatement("""
                        INSERT INTO ingestion_rate_counters (organization_id, minute_start, accepted_count)
                        VALUES (?, date_trunc('minute', clock_timestamp()) - interval '3 hours', 1),
                               (?, date_trunc('minute', clock_timestamp()), 1)
                        """)) {
            statement.setObject(1, owner);
            statement.setObject(2, owner);
            statement.executeUpdate();
        }
        assertThat(repository.deleteExpiredRateCounters(1)).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM ingestion_rate_counters WHERE organization_id = ?", owner))
                .isEqualTo(1);
    }

    @Test
    void enforcesPersistedOrganizationEventRateWithoutChargingDuplicates() throws Exception {
        String firstKey = "quota-" + UUID.randomUUID();
        ResponseEntity<JsonNode> first = publish("limited-key", firstKey, "payment.completed", Map.of());
        ResponseEntity<JsonNode> repeated = publish("limited-key", firstKey, "payment.completed", Map.of());
        ResponseEntity<JsonNode> limited = publish("limited-key", "quota-" + UUID.randomUUID(),
                "payment.completed", Map.of());
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(repeated.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(limited.getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(count("SELECT count(*) FROM events WHERE organization_id = ?", LIMITED_ORGANIZATION))
                .isEqualTo(1);
    }

    private ResponseEntity<JsonNode> publish(String apiKey, String key, String type, Map<String, Object> data) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(apiKey);
        headers.set("Idempotency-Key", key);
        return client.exchange("/v1/events", HttpMethod.POST,
                new HttpEntity<>(Map.of("event_type", type, "schema_version", 1, "data", data), headers),
                JsonNode.class);
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
}

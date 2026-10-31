package com.relayforge.deliveryapi;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.LocalDate;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class DeliverySummaryIT {
    private static final String CREDENTIAL = "test-summary-credential-01234567890123456789";
    private static final PostgreSQLContainer<?> DATABASE = new PostgreSQLContainer<>("postgres:17.11")
            .withDatabaseName("relayforge_delivery")
            .withUsername("relayforge")
            .withPassword("relayforge-test");

    static {
        DATABASE.start();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.flyway.url", DATABASE::getJdbcUrl);
        registry.add("spring.flyway.user", DATABASE::getUsername);
        registry.add("spring.flyway.password", DATABASE::getPassword);
        registry.add("relayforge.database.url", DATABASE::getJdbcUrl);
        registry.add("relayforge.database.username", DATABASE::getUsername);
        registry.add("relayforge.database.password", DATABASE::getPassword);
        registry.add("relayforge.event-summary.credential", () -> CREDENTIAL);
    }

    @Autowired
    private TestRestTemplate client;

    @Autowired
    private JdbcTemplate jdbc;

    @Test
    void returnsPendingUntilRoutingThenScopedDeliveryCounts() {
        UUID eventId = UUID.randomUUID();
        UUID organization = UUID.randomUUID();
        LocalDate day = jdbc.queryForObject("SELECT current_date", LocalDate.class);
        assertThat(read(organization, day, eventId, CREDENTIAL).path("status").asText())
                .isEqualTo("PENDING");
        jdbc.update("""
                INSERT INTO routing_progress (accepted_day, event_id, organization_id,
                    event_accepted_at, ingestion_shard_id, delivery_shard_id, status, target_count)
                VALUES (?, ?, ?, clock_timestamp(), 'ing-a', 'dlv-a', 'ROUTED', 2)
                """, day, eventId, organization);
        jdbc.update("""
                INSERT INTO deliveries (accepted_day, id, organization_id, event_id, endpoint_id, status)
                VALUES (?, ?, ?, ?, ?, 'SUCCEEDED'), (?, ?, ?, ?, ?, 'DEAD_LETTERED')
                """, day, UUID.randomUUID(), organization, eventId, UUID.randomUUID(),
                day, UUID.randomUUID(), organization, eventId, UUID.randomUUID());
        JsonNode summary = read(organization, day, eventId, CREDENTIAL);
        assertThat(summary.path("status").asText()).isEqualTo("ROUTED");
        assertThat(summary.path("target_count").asInt()).isEqualTo(2);
        assertThat(summary.path("delivery_count").asInt()).isEqualTo(2);
        assertThat(summary.path("succeeded_count").asInt()).isEqualTo(1);
        assertThat(summary.path("dead_lettered_count").asInt()).isEqualTo(1);
        assertThat(read(UUID.randomUUID(), day, eventId, CREDENTIAL).path("status").asText())
                .isEqualTo("PENDING");
    }

    @Test
    void rejectsMissingInternalCredential() {
        UUID organization = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        LocalDate day = jdbc.queryForObject("SELECT current_date", LocalDate.class);
        HttpHeaders headers = new HttpHeaders();
        assertThat(client.exchange(path(organization, day, eventId), HttpMethod.GET,
                new HttpEntity<>(headers), JsonNode.class).getStatusCode())
                .isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    private JsonNode read(UUID organization, LocalDate day, UUID eventId, String credential) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-RelayForge-Event-Summary-Credential", credential);
        var response = client.exchange(path(organization, day, eventId), HttpMethod.GET,
                new HttpEntity<>(headers), JsonNode.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        return response.getBody();
    }

    private String path(UUID organization, LocalDate day, UUID eventId) {
        return "/internal/v1/events/" + eventId + "/summary?organization_id=" + organization
                + "&accepted_day=" + day;
    }
}

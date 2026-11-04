package com.relayforge.deliveryapi.history;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

class DeliveryHistoryIT {
    private static final PostgreSQLContainer<?> DATABASE = new PostgreSQLContainer<>("postgres:17.11")
            .withDatabaseName("relayforge_delivery")
            .withUsername("relayforge")
            .withPassword("relayforge-test");
    private static final Instant NOW = Instant.now();
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
    private static JdbcTemplate jdbc;
    private static DeliveryQueryService service;

    @BeforeAll
    static void startDatabase() {
        DATABASE.start();
        Flyway.configure().dataSource(DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword())
                .locations("filesystem:../relayforge-delivery-api/src/main/resources/db/migration")
                .load().migrate();
        jdbc = new JdbcTemplate(new DriverManagerDataSource(DATABASE.getJdbcUrl(),
                DATABASE.getUsername(), DATABASE.getPassword()));
        service = new DeliveryQueryService(new JdbcDeliveryHistoryRepository(jdbc), CLOCK);
    }

    @Test
    void paginatesWithStableOrderAndScopesDetailAndAttemptsToOrganization() {
        UUID owner = UUID.randomUUID();
        UUID other = UUID.randomUUID();
        UUID endpoint = UUID.randomUUID();
        Instant createdAt = NOW.minusSeconds(60);
        String from = createdAt.minusSeconds(1).toString();
        String to = NOW.plusSeconds(1).toString();
        UUID first = UUID.fromString("00000000-0000-0000-0000-000000000001");
        UUID second = UUID.fromString("00000000-0000-0000-0000-000000000002");
        UUID third = UUID.fromString("00000000-0000-0000-0000-000000000003");
        insert(owner, endpoint, first, createdAt, "SUCCEEDED");
        insert(owner, endpoint, second, createdAt, "SUCCEEDED");
        insert(owner, endpoint, third, createdAt, "SUCCEEDED");
        insert(other, endpoint, UUID.randomUUID(), createdAt, "SUCCEEDED");
        jdbc.update("""
                INSERT INTO delivery_attempts (accepted_day, organization_id, delivery_id,
                    attempt_number, started_at, finished_at, outcome, http_status, duration_ms,
                    response_excerpt)
                VALUES (?, ?, ?, 1, ?, ?, 'SUCCEEDED', 204, 12, 'ok')
                """, createdAt.atZone(ZoneOffset.UTC).toLocalDate(), owner, third,
                Timestamp.from(createdAt), Timestamp.from(createdAt.plusMillis(12)));

        var firstPage = service.list(owner, "SUCCEEDED", endpoint.toString(),
                from, to, "2", null);
        assertThat(firstPage.data()).extracting(DeliveryQueryService.DeliveryView::deliveryId)
                .containsExactly(third, second);
        assertThat(firstPage.nextCursor()).isNotNull();
        var secondPage = service.list(owner, "SUCCEEDED", endpoint.toString(),
                null, null, "2", firstPage.nextCursor());
        assertThat(secondPage.data()).extracting(DeliveryQueryService.DeliveryView::deliveryId)
                .containsExactly(first);
        assertThat(secondPage.nextCursor()).isNull();
        assertThat(service.detail(owner, third).orElseThrow().attempts()).singleElement()
                .satisfies(attempt -> {
                    assertThat(attempt.attemptNumber()).isEqualTo(1);
                    assertThat(attempt.httpStatus()).isEqualTo(204);
                    assertThat(attempt.responseExcerpt()).isEqualTo("ok");
                });
        assertThat(service.detail(other, third)).isEmpty();
    }

    @Test
    void hidesExpiredAcceptanceCohortDuringPhysicalGrace() {
        Instant acceptedAt = NOW.minusSeconds(2160L * 3600 + 1);
        LocalDate day = acceptedAt.atZone(ZoneOffset.UTC).toLocalDate();
        for (String parent : new String[]{"routing_progress", "deliveries"}) {
            jdbc.execute("CREATE TABLE IF NOT EXISTS " + parent + "_"
                    + day.format(java.time.format.DateTimeFormatter.BASIC_ISO_DATE)
                    + " PARTITION OF " + parent + " FOR VALUES FROM ('" + day
                    + "') TO ('" + day.plusDays(1) + "')");
        }
        UUID owner = UUID.randomUUID();
        UUID event = UUID.randomUUID();
        UUID deliveryId = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO routing_progress (accepted_day, event_id, organization_id,
                    event_accepted_at, ingestion_shard_id, delivery_shard_id, status)
                VALUES (?, ?, ?, ?, 'ing-a', 'dlv-a', 'ROUTED')
                """, day, event, owner, Timestamp.from(acceptedAt));
        jdbc.update("""
                INSERT INTO deliveries (accepted_day, id, organization_id, event_id, endpoint_id,
                    status, created_at)
                VALUES (?, ?, ?, ?, ?, 'SUCCEEDED', ?)
                """, day, deliveryId, owner, event, UUID.randomUUID(), Timestamp.from(NOW));

        assertThat(service.detail(owner, deliveryId)).isEmpty();
        assertThat(service.list(owner, "SUCCEEDED", null,
                NOW.minusSeconds(3600).toString(), NOW.plusSeconds(1).toString(), "10", null)
                .data()).isEmpty();
    }

    private static void insert(UUID organization, UUID endpoint, UUID delivery,
            Instant createdAt, String status) {
        UUID event = UUID.randomUUID();
        LocalDate day = createdAt.atZone(ZoneOffset.UTC).toLocalDate();
        jdbc.update("""
                INSERT INTO routing_progress (accepted_day, event_id, organization_id,
                    event_accepted_at, ingestion_shard_id, delivery_shard_id, status)
                VALUES (?, ?, ?, ?, 'ing-a', 'dlv-a', 'ROUTED')
                """, day, event, organization, Timestamp.from(createdAt));
        jdbc.update("""
                INSERT INTO deliveries (accepted_day, id, organization_id, event_id, endpoint_id,
                    status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, day, delivery, organization, event, endpoint,
                status, Timestamp.from(createdAt), Timestamp.from(createdAt));
    }
}

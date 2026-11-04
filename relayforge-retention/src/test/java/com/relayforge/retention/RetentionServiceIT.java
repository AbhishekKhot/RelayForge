package com.relayforge.retention;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.DriverManager;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

class RetentionServiceIT {
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.11")
            .withDatabaseName("relayforge_ingestion")
            .withUsername("relayforge")
            .withPassword("relayforge-test");
    private static final Instant NOW = Instant.parse("2026-11-04T12:00:00Z");
    private static final DateTimeFormatter DAY_NAME = DateTimeFormatter.BASIC_ISO_DATE;
    private static DataSource ingestion;
    private static DataSource delivery;
    private static JdbcTemplate ingestSql;
    private static JdbcTemplate deliverSql;

    @BeforeAll
    static void databases() throws Exception {
        POSTGRES.start();
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(),
                POSTGRES.getUsername(), POSTGRES.getPassword());
                var statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE relayforge_delivery");
        }
        ingestion = source("relayforge_ingestion");
        delivery = source("relayforge_delivery");
        Flyway.configure().dataSource(ingestion)
                .locations("filesystem:../relayforge-ingestion/src/main/resources/db/migration")
                .load().migrate();
        Flyway.configure().dataSource(delivery)
                .locations("filesystem:../relayforge-delivery-api/src/main/resources/db/migration")
                .load().migrate();
        ingestSql = new JdbcTemplate(ingestion);
        deliverSql = new JdbcTemplate(delivery);
    }

    @Test
    void blocksLiveWorkAndUnpublishedOutboxThenRemovesExpiredCohortOnRetry() {
        LocalDate oldDay = LocalDate.parse("2026-08-01");
        createOldPartitions(oldDay);
        UUID organization = UUID.randomUUID();
        UUID event = UUID.randomUUID();
        UUID deliveryId = UUID.randomUUID();
        UUID endpoint = UUID.randomUUID();
        Instant accepted = oldDay.atTime(12, 0).toInstant(ZoneOffset.UTC);
        deliverSql.update("""
                INSERT INTO endpoint_projection (organization_id, endpoint_id, status,
                    source_version, updated_at)
                VALUES (?, ?, 'ACTIVE', 1, ?)
                """, organization, endpoint, Timestamp.from(NOW));
        ingestSql.update("""
                INSERT INTO idempotency_registry (organization_id, idempotency_key, bucket_id,
                    request_hash, request_hash_version, event_id, event_locator, accepted_day,
                    accepted_at, expires_at)
                VALUES (?, 'old-key', 0, decode(repeat('aa', 32), 'hex'), 1, ?, 'old-locator',
                    ?, ?, ?)
                """, organization, event, oldDay, Timestamp.from(accepted),
                Timestamp.from(accepted.plusSeconds(2160L * 3600)));
        ingestSql.update("""
                INSERT INTO events (accepted_day, id, organization_id, event_type, payload,
                    payload_hash, idempotency_key, accepted_at)
                VALUES (?, ?, ?, 'order.created', '{}'::jsonb,
                    decode(repeat('aa', 32), 'hex'), 'old-key', ?)
                """, oldDay, event, organization, Timestamp.from(accepted));
        ingestSql.update("""
                INSERT INTO ingestion_outbox (organization_id, accepted_day, event_id,
                    message_type, topic, partition_key, payload)
                VALUES (?, ?, ?, 'event.accepted.v1', 'relayforge.event.accepted.v1',
                    'old-locator', '{}'::jsonb)
                """, organization, oldDay, event);
        deliverSql.update("""
                INSERT INTO routing_progress (accepted_day, event_id, organization_id,
                    event_accepted_at, ingestion_shard_id, delivery_shard_id, status)
                VALUES (?, ?, ?, ?, 'ing-a', 'dlv-a', 'ROUTED')
                """, oldDay, event, organization, Timestamp.from(accepted));
        deliverSql.update("""
                INSERT INTO deliveries (accepted_day, id, organization_id, event_id, endpoint_id)
                VALUES (?, ?, ?, ?, ?)
                """, oldDay, deliveryId, organization, event, endpoint);
        deliverSql.update("""
                INSERT INTO delivery_work (delivery_id, accepted_day, organization_id,
                    endpoint_id, status, event_expires_at, next_attempt_at)
                VALUES (?, ?, ?, ?, 'PENDING', ?, ?)
                """, deliveryId, oldDay, organization, endpoint,
                Timestamp.from(accepted.plusSeconds(2160L * 3600)), Timestamp.from(accepted));
        deliverSql.update("""
                INSERT INTO delivery_attempts (accepted_day, organization_id, delivery_id,
                    attempt_number, started_at, outcome)
                VALUES (?, ?, ?, 1, ?, 'SUCCESS')
                """, oldDay, organization, deliveryId, Timestamp.from(NOW));
        deliverSql.update("""
                INSERT INTO authentication_attempts (accepted_day, organization_id,
                    endpoint_id, delivery_id, attempt_number, started_at, outcome)
                VALUES (?, ?, ?, ?, 1, ?, 'SUCCESS')
                """, oldDay, organization, endpoint, deliveryId, Timestamp.from(NOW));

        var retention = new RetentionService(ingestion, delivery,
                Clock.fixed(NOW, ZoneOffset.UTC));
        retention.runOnce();
        assertThat(partitionExists(ingestSql, "events", oldDay)).isTrue();
        assertThat(partitionExists(deliverSql, "deliveries", oldDay)).isTrue();

        deliverSql.update("DELETE FROM delivery_work WHERE delivery_id = ?", deliveryId);
        deliverSql.update("UPDATE deliveries SET status = 'SUCCEEDED' WHERE id = ?", deliveryId);
        deliverSql.update("UPDATE delivery_outbox SET status = 'PUBLISHED', published_at = clock_timestamp() "
                + "WHERE accepted_day = ?", oldDay);
        retention.runOnce();
        assertThat(partitionExists(ingestSql, "events", oldDay)).isTrue();
        assertThat(partitionExists(deliverSql, "deliveries", oldDay)).isTrue();

        ingestSql.update("UPDATE ingestion_outbox SET status = 'PUBLISHED', "
                + "published_at = clock_timestamp() WHERE accepted_day = ?", oldDay);
        retention.runOnce();
        retention.runOnce();
        assertThat(partitionExists(ingestSql, "events", oldDay)).isFalse();
        assertThat(ingestSql.queryForObject("SELECT count(*) FROM idempotency_registry "
                + "WHERE organization_id = ?", Integer.class, organization)).isZero();
        assertThat(ingestSql.queryForObject("SELECT count(*) FROM ingestion_outbox "
                + "WHERE accepted_day = ?", Integer.class, oldDay)).isZero();
        assertThat(partitionExists(ingestSql, "events", NOW.atZone(ZoneOffset.UTC)
                .toLocalDate().plusDays(14))).isTrue();
        assertThat(partitionExists(deliverSql, "delivery_attempts", oldDay)).isFalse();
        assertThat(partitionExists(deliverSql, "authentication_attempts", oldDay)).isFalse();
        assertThat(deliverSql.queryForObject("SELECT count(*) FROM endpoint_projection "
                + "WHERE organization_id = ?", Integer.class, organization)).isEqualTo(1);
        ingestSql.update("""
                INSERT INTO idempotency_registry (organization_id, idempotency_key, bucket_id,
                    request_hash, request_hash_version, event_id, event_locator, accepted_day,
                    accepted_at, expires_at)
                VALUES (?, 'old-key', 0, decode(repeat('aa', 32), 'hex'), 1, ?, 'new-locator',
                    ?, ?, ?)
                """, organization, UUID.randomUUID(), NOW.atZone(ZoneOffset.UTC).toLocalDate(),
                Timestamp.from(NOW), Timestamp.from(NOW.plusSeconds(2160L * 3600)));
    }

    @Test
    void keepsPartiallyExpiredDayDuringPhysicalGrace() {
        LocalDate boundary = NOW.minusSeconds(2160L * 3600)
                .atZone(ZoneOffset.UTC).toLocalDate();
        createOldPartitions(boundary);

        new RetentionService(ingestion, delivery, Clock.fixed(NOW, ZoneOffset.UTC)).runOnce();

        assertThat(partitionExists(ingestSql, "events", boundary)).isTrue();
        assertThat(partitionExists(deliverSql, "deliveries", boundary)).isTrue();
    }

    @Test
    void waitsForRoutingAfterIngestionOutboxPublication() {
        LocalDate day = LocalDate.parse("2026-07-31");
        createOldPartitions(day);
        UUID organization = UUID.randomUUID();
        UUID event = UUID.randomUUID();
        Instant accepted = day.atTime(12, 0).toInstant(ZoneOffset.UTC);
        ingestSql.update("""
                INSERT INTO events (accepted_day, id, organization_id, event_type, payload,
                    payload_hash, idempotency_key, accepted_at)
                VALUES (?, ?, ?, 'order.created', '{}'::jsonb,
                    decode(repeat('aa', 32), 'hex'), 'routing-lag', ?)
                """, day, event, organization, Timestamp.from(accepted));
        ingestSql.update("""
                INSERT INTO ingestion_outbox (organization_id, accepted_day, event_id,
                    message_type, topic, partition_key, payload, status, published_at)
                VALUES (?, ?, ?, 'event.accepted.v1', 'relayforge.event.accepted.v1',
                    'routing-lag', '{}'::jsonb, 'PUBLISHED', ?)
                """, organization, day, event, Timestamp.from(NOW));
        var retention = new RetentionService(ingestion, delivery,
                Clock.fixed(NOW, ZoneOffset.UTC));

        assertThat(retention.runOnce().blockedDays()).isGreaterThan(0);
        assertThat(partitionExists(deliverSql, "routing_progress", day)).isTrue();

        deliverSql.update("""
                INSERT INTO routing_progress (accepted_day, event_id, organization_id,
                    event_accepted_at, ingestion_shard_id, delivery_shard_id, status)
                VALUES (?, ?, ?, ?, 'ing-a', 'dlv-a', 'NO_SUBSCRIBERS')
                """, day, event, organization, Timestamp.from(accepted));
        var report = retention.runOnce();
        assertThat(partitionExists(deliverSql, "routing_progress", day))
                .as("report=%s events=%s routed=%s ingestion_unpublished=%s "
                                + "delivery_unpublished=%s delivery_work=%s", report,
                        ingestSql.queryForObject("SELECT count(*) FROM events WHERE accepted_day = ?",
                                Integer.class, day),
                        deliverSql.queryForObject("SELECT count(*) FROM routing_progress "
                                + "WHERE accepted_day = ?", Integer.class, day),
                        ingestSql.queryForObject("SELECT count(*) FROM ingestion_outbox "
                                + "WHERE accepted_day = ? AND (status <> 'PUBLISHED' "
                                + "OR published_at IS NULL)", Integer.class, day),
                        deliverSql.queryForObject("SELECT count(*) FROM delivery_outbox "
                                + "WHERE accepted_day = ? AND (status <> 'PUBLISHED' "
                                + "OR published_at IS NULL)", Integer.class, day),
                        deliverSql.queryForObject("SELECT count(*) FROM delivery_work "
                                + "WHERE accepted_day = ?", Integer.class, day))
                .isFalse();
        assertThat(partitionExists(ingestSql, "events", day)).isFalse();
    }

    private static DataSource source(String database) {
        String url = POSTGRES.getJdbcUrl().replace("/relayforge_ingestion", "/" + database);
        return new DriverManagerDataSource(url, POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    private static void createOldPartitions(LocalDate day) {
        create(ingestSql, "events", day);
        for (String table : new String[]{"routing_progress", "routing_targets", "deliveries",
                "delivery_attempts", "authentication_attempts", "replay_requests",
                "delivery_audit_records"}) {
            create(deliverSql, table, day);
        }
    }

    private static void create(JdbcTemplate jdbc, String parent, LocalDate day) {
        jdbc.execute("CREATE TABLE IF NOT EXISTS " + parent + "_" + DAY_NAME.format(day)
                + " PARTITION OF " + parent + " FOR VALUES FROM ('" + day + "') TO ('"
                + day.plusDays(1) + "')");
    }

    private static boolean partitionExists(JdbcTemplate jdbc, String parent, LocalDate day) {
        return jdbc.queryForObject("SELECT to_regclass(?) IS NOT NULL", Boolean.class,
                "public." + parent + "_" + DAY_NAME.format(day));
    }
}

package com.relayforge.deliveryapi.replay;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.Executors;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;
import org.testcontainers.containers.PostgreSQLContainer;

class DeliveryReplayIT {
    private static final PostgreSQLContainer<?> DATABASE = new PostgreSQLContainer<>("postgres:17.11")
            .withDatabaseName("relayforge_delivery")
            .withUsername("relayforge")
            .withPassword("relayforge-test");
    private static JdbcTemplate jdbc;
    private static DeliveryReplayService service;
    private static final Instant NOW = Instant.now();
    private static final UUID ACTOR = UUID.randomUUID();

    @BeforeAll
    static void startDatabase() {
        DATABASE.start();
        Flyway.configure().dataSource(DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword())
                .locations("filesystem:../relayforge-delivery-api/src/main/resources/db/migration")
                .load().migrate();
        var source = new DriverManagerDataSource(DATABASE.getJdbcUrl(),
                DATABASE.getUsername(), DATABASE.getPassword());
        jdbc = new JdbcTemplate(source);
        service = new DeliveryReplayService(jdbc,
                new TransactionTemplate(new DataSourceTransactionManager(source)),
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    void createsOneAtomicReplayAndReturnsSameResultForSameKey() {
        Source source = source("DEAD_LETTERED", "ACTIVE", NOW.minusSeconds(60));
        var first = service.replay(source.organization(), ACTOR, source.delivery(),
                "retry-key", "Outage resolved", "req-one");
        var repeated = service.replay(source.organization(), ACTOR, source.delivery(),
                "retry-key", "Outage resolved", "req-two");
        assertThat(repeated).isEqualTo(first);
        assertThat(first.originalDeliveryId()).isEqualTo(source.delivery());
        assertThat(first.replayDeliveryId()).isNotEqualTo(source.delivery());
        assertThat(first.generation()).isEqualTo(2);
        assertThat(first.status()).isEqualTo("PENDING");
        assertThat(count("deliveries", "id", first.replayDeliveryId())).isEqualTo(1);
        assertThat(count("delivery_work", "delivery_id", first.replayDeliveryId())).isEqualTo(1);
        assertThat(count("replay_requests", "replay_delivery_id", first.replayDeliveryId())).isEqualTo(1);
        assertThat(count("delivery_audit_records", "delivery_id", first.replayDeliveryId())).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT actor_key_id FROM delivery_audit_records WHERE delivery_id = ?",
                UUID.class, first.replayDeliveryId())).isEqualTo(ACTOR);
        assertThat(jdbc.queryForObject("SELECT requested_by_key_id FROM replay_requests "
                + "WHERE replay_delivery_id = ?", UUID.class, first.replayDeliveryId())).isEqualTo(ACTOR);
        assertThat(count("delivery_outbox", "delivery_id", first.replayDeliveryId())).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT status FROM deliveries WHERE id = ?", String.class,
                source.delivery())).isEqualTo("DEAD_LETTERED");
        assertThat(jdbc.queryForObject("SELECT payload ->> 'delivery_id' FROM delivery_outbox "
                + "WHERE delivery_id = ? AND message_type = 'delivery.ready.v1'",
                String.class, first.replayDeliveryId()))
                .isEqualTo(first.replayDeliveryId().toString());
    }

    @Test
    void rejectsChangedBodyAndOtherOrganization() {
        Source source = source("DEAD_LETTERED", "ACTIVE", NOW.minusSeconds(60));
        service.replay(source.organization(), ACTOR, source.delivery(), "same-key", "one", "req");
        assertStatus(HttpStatus.CONFLICT, () -> service.replay(source.organization(), ACTOR,
                source.delivery(), "same-key", "two", "req"));
        assertStatus(HttpStatus.NOT_FOUND, () -> service.replay(UUID.randomUUID(), ACTOR,
                source.delivery(), "same-key", "one", "req"));
    }

    @Test
    void concurrentSameKeyCreatesOneGeneration() throws Exception {
        Source source = source("DEAD_LETTERED", "ACTIVE", NOW.minusSeconds(60));
        try (var workers = Executors.newFixedThreadPool(2)) {
            var first = workers.submit(() -> service.replay(source.organization(), ACTOR, source.delivery(),
                    "parallel", "Recovered", "req-a"));
            var second = workers.submit(() -> service.replay(source.organization(), ACTOR, source.delivery(),
                    "parallel", "Recovered", "req-b"));
            assertThat(first.get()).isEqualTo(second.get());
        }
        assertThat(jdbc.queryForObject("SELECT count(*) FROM replay_requests WHERE original_delivery_id = ?",
                Integer.class, source.delivery())).isEqualTo(1);
    }

    @Test
    void rejectsIneligibleExpiredAndInactiveEndpoint() {
        Source pending = source("PENDING", "ACTIVE", NOW.minusSeconds(60));
        assertStatus(HttpStatus.CONFLICT, () -> service.replay(pending.organization(), ACTOR,
                pending.delivery(), "pending", null, "req"));
        Source inactive = source("DEAD_LETTERED", "DISABLED", NOW.minusSeconds(60));
        assertStatus(HttpStatus.CONFLICT, () -> service.replay(inactive.organization(), ACTOR,
                inactive.delivery(), "disabled", null, "req"));
        Source expired = source("DEAD_LETTERED", "ACTIVE", NOW.minusSeconds(2161 * 3600L));
        assertStatus(HttpStatus.CONFLICT, () -> service.replay(expired.organization(), ACTOR,
                expired.delivery(), "expired", null, "req"));
    }

    @Test
    void rejectsReplayBeyondConfiguredGenerationLimit() {
        Source source = source("DEAD_LETTERED", "ACTIVE", NOW.minusSeconds(60));
        assertStatus(HttpStatus.TOO_MANY_REQUESTS, () -> service.replay(source.organization(), ACTOR,
                source.delivery(), "quota", null, "req", 1));
        assertThat(count("replay_requests", "original_delivery_id", source.delivery())).isZero();
    }

    @Test
    void rollsBackEveryRowWhenReadyOutboxFails() {
        Source source = source("DEAD_LETTERED", "ACTIVE", NOW.minusSeconds(60));
        jdbc.execute("""
                CREATE FUNCTION reject_replay_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN RAISE EXCEPTION 'outbox unavailable'; END $$
                """);
        jdbc.execute("CREATE TRIGGER reject_replay_outbox BEFORE INSERT ON delivery_outbox "
                + "FOR EACH ROW EXECUTE FUNCTION reject_replay_outbox()");
        try {
            assertStatus(HttpStatus.SERVICE_UNAVAILABLE, () -> service.replay(source.organization(), ACTOR,
                    source.delivery(), "rollback", null, "req"));
        } finally {
            jdbc.execute("DROP TRIGGER reject_replay_outbox ON delivery_outbox");
            jdbc.execute("DROP FUNCTION reject_replay_outbox()");
        }
        assertThat(jdbc.queryForObject("SELECT count(*) FROM deliveries WHERE organization_id = ? "
                + "AND event_id = (SELECT event_id FROM deliveries WHERE id = ?)", Integer.class,
                source.organization(), source.delivery())).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM replay_requests WHERE original_delivery_id = ?",
                Integer.class, source.delivery())).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM delivery_audit_records WHERE organization_id = ?",
                Integer.class, source.organization())).isZero();
    }

    private static void assertStatus(HttpStatus status, Runnable action) {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(ResponseStatusException.class,
                failure -> assertThat(failure.getStatusCode()).isEqualTo(status));
    }

    private static int count(String table, String column, UUID delivery) {
        String filter = table.equals("delivery_outbox")
                ? " AND message_type = 'delivery.ready.v1'" : "";
        return jdbc.queryForObject("SELECT count(*) FROM " + table + " WHERE " + column + " = ?"
                + filter,
                Integer.class, delivery);
    }

    private static Source source(String status, String endpointStatus, Instant acceptedAt) {
        UUID organization = UUID.randomUUID();
        UUID endpoint = UUID.randomUUID();
        UUID event = UUID.randomUUID();
        UUID delivery = UUID.randomUUID();
        LocalDate day = acceptedAt.atZone(ZoneOffset.UTC).toLocalDate();
        if (day.isBefore(LocalDate.now(ZoneOffset.UTC).minusDays(1))) {
            for (String table : new String[] {"routing_progress", "deliveries"}) {
                jdbc.execute("CREATE TABLE IF NOT EXISTS " + table + "_" + day.toString().replace("-", "")
                        + " PARTITION OF " + table + " FOR VALUES FROM ('" + day + "') TO ('"
                        + day.plusDays(1) + "')");
            }
        }
        jdbc.update("INSERT INTO endpoint_projection (organization_id, endpoint_id, status, source_version, updated_at) "
                + "VALUES (?, ?, ?, 1, clock_timestamp())", organization, endpoint, endpointStatus);
        jdbc.update("INSERT INTO routing_progress (accepted_day, event_id, organization_id, event_accepted_at, "
                + "ingestion_shard_id, delivery_shard_id, status) VALUES (?, ?, ?, ?, 'ing-a', 'dlv-a', 'ROUTED')",
                day, event, organization, Timestamp.from(acceptedAt));
        jdbc.update("INSERT INTO deliveries (accepted_day, id, organization_id, event_id, endpoint_id, status) "
                + "VALUES (?, ?, ?, ?, ?, ?)", day, delivery, organization, event, endpoint, status);
        return new Source(organization, delivery);
    }

    private record Source(UUID organization, UUID delivery) {
    }
}

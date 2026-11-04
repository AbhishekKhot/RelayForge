package com.relayforge.retryscheduler;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;

class RetrySchedulerServiceIT {
    private static final PostgreSQLContainer<?> DATABASE = new PostgreSQLContainer<>("postgres:17.11")
            .withDatabaseName("relayforge_delivery")
            .withUsername("relayforge")
            .withPassword("relayforge-test");
    private static JdbcTemplate jdbc;
    private static RetrySchedulerService scheduler;

    @BeforeAll
    static void startDatabase() {
        DATABASE.start();
        Flyway.configure().dataSource(DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword())
                .locations("filesystem:../relayforge-delivery-api/src/main/resources/db/migration")
                .load().migrate();
        var dataSource = new DriverManagerDataSource(DATABASE.getJdbcUrl(), DATABASE.getUsername(),
                DATABASE.getPassword());
        jdbc = new JdbcTemplate(dataSource);
        scheduler = new RetrySchedulerService(jdbc,
                new TransactionTemplate(new JdbcTransactionManager(dataSource)));
    }

    @BeforeEach
    void clearWork() {
        jdbc.execute("TRUNCATE TABLE delivery_work, deliveries, routing_targets, routing_progress CASCADE");
    }

    @Test
    void enqueuesOnlyDueRetryOnceWithOneReadyOutboxRecord() {
        UUID due = retryWork(false, true);
        UUID future = retryWork(false, false);
        assertThat(scheduler.scanDue(10)).isEqualTo(1);
        assertThat(scheduler.scanDue(10)).isZero();
        assertThat(state(due)).isEqualTo("ENQUEUED:ENQUEUED");
        assertThat(state(future)).isEqualTo("RETRY_SCHEDULED:RETRY_SCHEDULED");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM delivery_outbox WHERE delivery_id = ? AND message_type = 'delivery.ready.v1'",
                Integer.class, due)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM delivery_outbox WHERE delivery_id = ? AND message_type = 'delivery.ready.v1'",
                Integer.class, future)).isZero();
    }

    @Test
    void competingScannersDoNotDoubleSchedule() throws Exception {
        UUID delivery = retryWork(false, true);
        CountDownLatch start = new CountDownLatch(1);
        try (var workers = Executors.newFixedThreadPool(2)) {
            var first = workers.submit(() -> {
                start.await();
                return scheduler.scanDue(1);
            });
            var second = workers.submit(() -> {
                start.await();
                return scheduler.scanDue(1);
            });
            start.countDown();
            assertThat(first.get() + second.get()).isEqualTo(1);
        }
        assertThat(jdbc.queryForObject("SELECT count(*) FROM delivery_outbox WHERE delivery_id = ? AND message_type = 'delivery.ready.v1'",
                Integer.class, delivery)).isEqualTo(1);
    }

    @Test
    void rollsBackStateWhenReadyOutboxInsertFails() {
        UUID delivery = retryWork(false, true);
        jdbc.execute("ALTER TABLE delivery_outbox ADD CONSTRAINT reject_ready_test "
                + "CHECK (false) NOT VALID");
        try {
            assertThatThrownBy(() -> scheduler.scanDue(1))
                    .isInstanceOf(DataAccessException.class);
            assertThat(state(delivery)).isEqualTo("RETRY_SCHEDULED:RETRY_SCHEDULED");
            assertThat(jdbc.queryForObject(
                    "SELECT count(*) FROM delivery_outbox WHERE delivery_id = ? AND message_type = 'delivery.ready.v1'",
                    Integer.class, delivery)).isZero();
        } finally {
            jdbc.execute("ALTER TABLE delivery_outbox DROP CONSTRAINT reject_ready_test");
        }
    }

    @Test
    void expiresWorkWithoutPublishingAnotherSend() {
        UUID delivery = retryWork(true, true);
        assertThat(scheduler.scanDue(10)).isZero();
        assertThat(jdbc.queryForObject("SELECT status FROM deliveries WHERE id = ?",
                String.class, delivery)).isEqualTo("DEAD_LETTERED");
        assertThat(jdbc.queryForObject("SELECT last_error_code FROM deliveries WHERE id = ?",
                String.class, delivery)).isEqualTo("EVENT_EXPIRED");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM delivery_work WHERE delivery_id = ?",
                Integer.class, delivery)).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM delivery_outbox WHERE delivery_id = ? AND message_type = 'delivery.ready.v1'",
                Integer.class, delivery)).isZero();
    }

    @Test
    void reportsDueBacklogAndClearsItAfterEnqueue() {
        retryWork(false, true);
        var registry = new SimpleMeterRegistry();
        RetrySchedulerMetrics metrics = new RetrySchedulerMetrics(jdbc, registry);
        metrics.refresh();
        assertThat(registry.get("relayforge.delivery.backlog").gauge().value())
                .isEqualTo(1);
        assertThat(registry.get("relayforge.delivery.oldest.pending.age.seconds")
                .gauge().value()).isGreaterThanOrEqualTo(0);
        assertThat(registry.get("relayforge.delivery.retry.due.backlog").gauge().value())
                .isEqualTo(1);
        assertThat(registry.get("relayforge.delivery.retry.oldest.due.age.seconds")
                .gauge().value()).isGreaterThanOrEqualTo(0);
        scheduler.scanDue(10);
        metrics.refresh();
        assertThat(registry.get("relayforge.delivery.retry.due.backlog").gauge().value())
                .isZero();
    }

    private String state(UUID delivery) {
        return jdbc.queryForObject("""
                SELECT work.status || ':' || delivery.status FROM delivery_work work
                JOIN deliveries delivery ON delivery.accepted_day = work.accepted_day
                    AND delivery.id = work.delivery_id WHERE work.delivery_id = ?
                """, String.class, delivery);
    }

    private UUID retryWork(boolean expired, boolean due) {
        UUID organization = UUID.randomUUID();
        UUID event = UUID.randomUUID();
        UUID endpoint = UUID.randomUUID();
        UUID delivery = UUID.randomUUID();
        LocalDate day = LocalDate.now(ZoneOffset.UTC);
        jdbc.update("""
                INSERT INTO routing_progress (accepted_day, event_id, organization_id,
                    event_locator, event_accepted_at, ingestion_shard_id, delivery_shard_id, status)
                VALUES (?, ?, ?, ?, ?, 'ing-a', 'dlv-a', 'ROUTED')
                """, day, event, organization, "evt_" + event.toString().replace("-", ""),
                Timestamp.from(day.atStartOfDay().toInstant(ZoneOffset.UTC)));
        jdbc.update("""
                INSERT INTO deliveries (accepted_day, id, organization_id, event_id,
                    endpoint_id, status, attempt_count, next_attempt_at)
                VALUES (?, ?, ?, ?, ?, 'RETRY_SCHEDULED', 1,
                    clock_timestamp() + (? * interval '1 minute'))
                """, day, delivery, organization, event, endpoint, due ? -1 : 5);
        jdbc.update("""
                INSERT INTO delivery_work (delivery_id, accepted_day, organization_id,
                    endpoint_id, status, event_expires_at, next_attempt_at)
                VALUES (?, ?, ?, ?, 'RETRY_SCHEDULED',
                    clock_timestamp() + (? * interval '1 minute'),
                    clock_timestamp() + (? * interval '1 minute'))
                """, delivery, day, organization, endpoint, expired ? -1 : 30, due ? -1 : 5);
        return delivery;
    }
}

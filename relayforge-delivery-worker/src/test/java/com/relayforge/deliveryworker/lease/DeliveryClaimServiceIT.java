package com.relayforge.deliveryworker.lease;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;

class DeliveryClaimServiceIT {
    private static final PostgreSQLContainer<?> DATABASE = new PostgreSQLContainer<>("postgres:17.11")
            .withDatabaseName("relayforge_delivery")
            .withUsername("relayforge")
            .withPassword("relayforge-test");
    private static JdbcTemplate jdbc;
    private static DeliveryClaimService claims;
    private static DeliveryAttemptService attempts;

    @BeforeAll
    static void startDatabase() {
        DATABASE.start();
        Flyway.configure().dataSource(DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword())
                .locations("filesystem:../relayforge-delivery-api/src/main/resources/db/migration")
                .load().migrate();
        var dataSource = new DriverManagerDataSource(DATABASE.getJdbcUrl(), DATABASE.getUsername(),
                DATABASE.getPassword());
        jdbc = new JdbcTemplate(dataSource);
        claims = new DeliveryClaimService(new JdbcDeliveryLeaseRepository(jdbc),
                new TransactionTemplate(new JdbcTransactionManager(dataSource)));
        attempts = new DeliveryAttemptService(jdbc,
                new TransactionTemplate(new JdbcTransactionManager(dataSource)));
    }

    @BeforeEach
    void clearWork() {
        jdbc.execute("TRUNCATE TABLE delivery_work, deliveries, routing_targets, routing_progress CASCADE");
    }

    @Test
    void competingWorkersClaimOneDeliveryExactlyOnce() throws Exception {
        UUID delivery = due(UUID.randomUUID());
        CountDownLatch start = new CountDownLatch(1);
        try (var workers = Executors.newFixedThreadPool(2)) {
            var first = workers.submit(() -> {
                start.await();
                return claims.claim(delivery, "worker-one", 2, 20);
            });
            var second = workers.submit(() -> {
                start.await();
                return claims.claim(delivery, "worker-two", 2, 20);
            });
            start.countDown();
            assertThat(first.get().isPresent() ^ second.get().isPresent()).isTrue();
        }
        assertThat(jdbc.queryForObject("SELECT claim_count FROM deliveries WHERE id = ?", Integer.class,
                delivery)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT attempt_count FROM deliveries WHERE id = ?", Integer.class,
                delivery)).isZero();
        assertThat(jdbc.queryForObject("SELECT status FROM delivery_work WHERE delivery_id = ?", String.class,
                delivery)).isEqualTo("IN_PROGRESS");
    }

    @Test
    void recordsTokenAttemptsSeparatelyAndDefersWithoutWebhookAttempt() {
        UUID delivery = due(UUID.randomUUID());
        DeliveryClaimService.Lease lease = claims.claim(delivery, "worker-auth", 2, 20).orElseThrow();
        Instant started = Instant.now().minusMillis(20);
        assertThat(attempts.recordAuthentication(lease, "RETRYABLE_FAILURE", 500,
                "AUTH_HTTP_RETRYABLE", started)).isTrue();
        assertThat(attempts.recordAuthentication(lease, "SUCCEEDED", 200,
                null, started)).isTrue();
        assertThat(jdbc.queryForList("SELECT outcome FROM authentication_attempts WHERE delivery_id = ? "
                + "ORDER BY attempt_number", String.class, delivery))
                .containsExactly("RETRYABLE_FAILURE", "SUCCEEDED");
        assertThat(jdbc.queryForObject("SELECT attempt_count FROM deliveries WHERE id = ?",
                Integer.class, delivery)).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM delivery_attempts WHERE delivery_id = ?",
                Integer.class, delivery)).isZero();
        assertThat(attempts.deferClaimed(lease, Instant.now().plusSeconds(30),
                "AUTH_RETRIES_EXHAUSTED")).isTrue();
        assertThat(jdbc.queryForObject("SELECT status FROM delivery_work WHERE delivery_id = ?",
                String.class, delivery)).isEqualTo("RETRY_SCHEDULED");
        assertThat(jdbc.queryForObject("SELECT status FROM deliveries WHERE id = ?",
                String.class, delivery)).isEqualTo("RETRY_SCHEDULED");
        assertThat(attempts.recordAuthentication(lease, "SUCCEEDED", 200, null, started)).isFalse();
    }

    @Test
    void expiryRecoveryFencesOldTokenAndAllowsNewOwner() {
        UUID delivery = due(UUID.randomUUID());
        DeliveryClaimService.Lease old = claims.claim(delivery, "worker-old", 2, 20).orElseThrow();
        jdbc.update("UPDATE delivery_work SET lease_expires_at = clock_timestamp() - interval '1 second' "
                + "WHERE delivery_id = ?", delivery);
        jdbc.update("UPDATE deliveries SET lease_expires_at = clock_timestamp() - interval '1 second' "
                + "WHERE id = ?", delivery);
        assertThat(claims.recoverExpired(10)).isEqualTo(1);
        assertThat(claims.renew(old)).isEmpty();
        DeliveryClaimService.Lease current = claims.claim(delivery, "worker-new", 2, 20).orElseThrow();
        assertThat(current.token()).isNotEqualTo(old.token());
        assertThat(claims.owns(old)).isFalse();
        assertThat(claims.owns(current)).isTrue();
        assertThat(jdbc.queryForObject("SELECT claim_count FROM deliveries WHERE id = ?", Integer.class,
                delivery)).isEqualTo(2);
    }

    @Test
    void renewalKeepsSlowProcessingOwnedButStopsAtDeadline() {
        UUID delivery = due(UUID.randomUUID());
        DeliveryClaimService.Lease lease = claims.claim(delivery, "worker-slow", 2, 20).orElseThrow();
        assertThat(Duration.between(lease.claimedAt(), lease.deadline()).toSeconds()).isEqualTo(240);
        lease = claims.renew(lease).orElseThrow();
        assertThat(claims.recoverExpired(10)).isZero();
        assertThat(claims.owns(lease)).isTrue();
        jdbc.update("UPDATE delivery_work SET processing_deadline_at = clock_timestamp() - interval '1 second' "
                + "WHERE delivery_id = ?", delivery);
        assertThat(claims.renew(lease)).isEmpty();
    }

    @Test
    void enforcesEndpointAndPlatformConcurrency() {
        UUID endpoint = UUID.randomUUID();
        UUID first = due(endpoint);
        UUID second = due(endpoint);
        UUID third = due(UUID.randomUUID());
        assertThat(claims.claim(first, "worker-one", 1, 1)).isPresent();
        assertThat(claims.claim(second, "worker-two", 1, 20)).isEmpty();
        assertThat(claims.claim(third, "worker-three", 2, 1)).isEmpty();
    }

    @Test
    void reservesAttemptBeforeSendAndFinalizesSuccessfulDelivery() {
        UUID delivery = due(UUID.randomUUID());
        DeliveryClaimService.Lease lease = claims.claim(delivery, "worker-send", 2, 20).orElseThrow();
        assertThat(attempts.eventLocator(lease)).startsWith("evt_");
        DeliveryAttemptService.Attempt attempt = attempts.reserve(lease, 5);
        assertThat(attempt.number()).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT attempt_count FROM deliveries WHERE id = ?",
                Integer.class, delivery)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT outcome FROM delivery_attempts WHERE delivery_id = ?",
                String.class, delivery)).isEqualTo("IN_PROGRESS");
        assertThat(attempts.finish(lease, attempt, 204, null, null, null)).isTrue();
        assertThat(jdbc.queryForObject("SELECT status FROM deliveries WHERE id = ?",
                String.class, delivery)).isEqualTo("SUCCEEDED");
        assertThat(jdbc.queryForObject("SELECT http_status FROM delivery_attempts WHERE delivery_id = ?",
                Integer.class, delivery)).isEqualTo(204);
        assertThat(jdbc.queryForObject("SELECT duration_ms FROM delivery_attempts WHERE delivery_id = ?",
                Integer.class, delivery)).isNotNegative();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM delivery_work WHERE delivery_id = ?",
                Integer.class, delivery)).isZero();
    }

    @Test
    void staleLeaseCannotFinalizeReservedAttempt() {
        UUID delivery = due(UUID.randomUUID());
        DeliveryClaimService.Lease lease = claims.claim(delivery, "worker-stale", 2, 20).orElseThrow();
        DeliveryAttemptService.Attempt attempt = attempts.reserve(lease, 5);
        jdbc.update("UPDATE delivery_work SET lease_expires_at = clock_timestamp() - interval '1 second' "
                + "WHERE delivery_id = ?", delivery);
        assertThat(attempts.finish(lease, attempt, 200, null, null, null)).isFalse();
        assertThat(jdbc.queryForObject("SELECT outcome FROM delivery_attempts WHERE delivery_id = ?",
                String.class, delivery)).isEqualTo("IN_PROGRESS");
    }

    @Test
    void retryReservationSchedulesNewReadyMessage() {
        UUID delivery = due(UUID.randomUUID());
        DeliveryClaimService.Lease lease = claims.claim(delivery, "worker-retry", 2, 20).orElseThrow();
        DeliveryAttemptService.Attempt attempt = attempts.reserve(lease, 5);
        Instant retryAt = Instant.now().plusSeconds(120)
                .truncatedTo(java.time.temporal.ChronoUnit.MICROS);
        assertThat(attempts.finish(lease, attempt, 503, "HTTP_RETRYABLE", null,
                retryAt)).isTrue();
        assertThat(jdbc.queryForObject("SELECT status FROM deliveries WHERE id = ?",
                String.class, delivery)).isEqualTo("RETRY_SCHEDULED");
        assertThat(jdbc.queryForObject("SELECT status FROM delivery_work WHERE delivery_id = ?",
                String.class, delivery)).isEqualTo("RETRY_SCHEDULED");
        assertThat(jdbc.queryForObject("SELECT next_attempt_at FROM deliveries WHERE id = ?",
                Timestamp.class, delivery).toInstant()).isEqualTo(retryAt);
        assertThat(jdbc.queryForObject("SELECT next_attempt_at FROM delivery_work WHERE delivery_id = ?",
                Timestamp.class, delivery).toInstant()).isEqualTo(retryAt);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM delivery_outbox WHERE delivery_id = ?",
                Integer.class, delivery)).isZero();
    }

    @Test
    void recoveryMarksUnfinishedAttemptUnknownAndConsumesItsSlot() {
        UUID delivery = due(UUID.randomUUID());
        DeliveryClaimService.Lease old = claims.claim(delivery, "worker-crashed", 2, 20).orElseThrow();
        DeliveryAttemptService.Attempt unfinished = attempts.reserve(old, 5);
        jdbc.update("UPDATE delivery_work SET lease_expires_at = clock_timestamp() - interval '1 second' "
                + "WHERE delivery_id = ?", delivery);
        jdbc.update("UPDATE deliveries SET lease_expires_at = clock_timestamp() - interval '1 second' "
                + "WHERE id = ?", delivery);
        assertThat(claims.recoverExpired(10)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT outcome FROM delivery_attempts WHERE id = ?",
                String.class, unfinished.id())).isEqualTo("UNKNOWN");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM delivery_outbox WHERE delivery_id = ?",
                Integer.class, delivery)).isZero();
        DeliveryClaimService.Lease renewed = claims.claim(delivery, "worker-recovered", 2, 20)
                .orElseThrow();
        DeliveryAttemptService.Attempt next = attempts.reserve(renewed, 5);
        assertThat(next.number()).isEqualTo(2);
        assertThat(jdbc.queryForList("""
                SELECT delivery_id FROM delivery_attempts WHERE delivery_id = ?
                ORDER BY attempt_number
                """, UUID.class, delivery)).containsExactly(delivery, delivery);
    }

    @Test
    void expiredEventAfterCrashIsDeadLetteredWithoutAnotherSend() {
        UUID delivery = due(UUID.randomUUID());
        DeliveryClaimService.Lease lease = claims.claim(delivery, "worker-expired", 2, 20)
                .orElseThrow();
        DeliveryAttemptService.Attempt unfinished = attempts.reserve(lease, 5);
        jdbc.update("UPDATE delivery_work SET event_expires_at = clock_timestamp() - interval '1 second', "
                + "lease_expires_at = clock_timestamp() - interval '1 second' WHERE delivery_id = ?",
                delivery);
        jdbc.update("UPDATE deliveries SET lease_expires_at = clock_timestamp() - interval '1 second' "
                + "WHERE id = ?", delivery);
        assertThat(claims.recoverExpired(10)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT outcome FROM delivery_attempts WHERE id = ?",
                String.class, unfinished.id())).isEqualTo("UNKNOWN");
        assertThat(jdbc.queryForObject("SELECT status FROM deliveries WHERE id = ?",
                String.class, delivery)).isEqualTo("DEAD_LETTERED");
        assertThat(jdbc.queryForObject("SELECT last_error_code FROM deliveries WHERE id = ?",
                String.class, delivery)).isEqualTo("EVENT_EXPIRED");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM delivery_work WHERE delivery_id = ?",
                Integer.class, delivery)).isZero();
    }

    @Test
    void unavailableConfigurationDefersUnclaimedWorkWithoutHttpAttempt() {
        UUID delivery = due(UUID.randomUUID());
        attempts.deferUnclaimed(delivery, Instant.now().plusSeconds(30));
        assertThat(jdbc.queryForObject("SELECT attempt_count FROM deliveries WHERE id = ?",
                Integer.class, delivery)).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM delivery_outbox WHERE delivery_id = ?",
                Integer.class, delivery)).isEqualTo(1);
        assertThat(claims.claim(delivery, "worker-early", 2, 20)).isEmpty();
    }

    @Test
    void exhaustedAttemptLimitTerminatesWithoutSendingAgain() {
        UUID delivery = due(UUID.randomUUID());
        jdbc.update("UPDATE deliveries SET attempt_count = 3 WHERE id = ?", delivery);
        attempts.finishExhausted(delivery, 3);
        assertThat(jdbc.queryForObject("SELECT status FROM deliveries WHERE id = ?",
                String.class, delivery)).isEqualTo("DEAD_LETTERED");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM delivery_work WHERE delivery_id = ?",
                Integer.class, delivery)).isZero();
    }

    @Test
    void rejectsInvalidUnclaimedConfigurationWithoutConsumingHttpAttempt() {
        UUID delivery = due(UUID.randomUUID());
        assertThat(attempts.rejectUnclaimed(delivery, "CONFIGURATION_INVALID")).isTrue();
        assertThat(jdbc.queryForObject("SELECT status FROM deliveries WHERE id = ?",
                String.class, delivery)).isEqualTo("DEAD_LETTERED");
        assertThat(jdbc.queryForObject("SELECT last_error_code FROM deliveries WHERE id = ?",
                String.class, delivery)).isEqualTo("CONFIGURATION_INVALID");
        assertThat(jdbc.queryForObject("SELECT attempt_count FROM deliveries WHERE id = ?",
                Integer.class, delivery)).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM delivery_work WHERE delivery_id = ?",
                Integer.class, delivery)).isZero();
    }

    @Test
    void rejectsInvalidClaimedDestinationOnlyWhileLeaseIsOwned() {
        UUID delivery = due(UUID.randomUUID());
        DeliveryClaimService.Lease lease = claims.claim(delivery, "worker-invalid", 2, 20)
                .orElseThrow();
        assertThat(attempts.rejectClaimed(lease, "DESTINATION_REJECTED")).isTrue();
        assertThat(jdbc.queryForObject("SELECT status FROM deliveries WHERE id = ?",
                String.class, delivery)).isEqualTo("DEAD_LETTERED");
        assertThat(jdbc.queryForObject("SELECT attempt_count FROM deliveries WHERE id = ?",
                Integer.class, delivery)).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM delivery_work WHERE delivery_id = ?",
                Integer.class, delivery)).isZero();
    }

    private static UUID due(UUID endpoint) {
        UUID organization = UUID.randomUUID();
        UUID event = UUID.randomUUID();
        UUID delivery = UUID.randomUUID();
        LocalDate day = LocalDate.now(ZoneOffset.UTC);
        jdbc.update("""
                INSERT INTO routing_progress (accepted_day, event_id, organization_id,
                    event_locator, event_accepted_at, ingestion_shard_id, delivery_shard_id, status)
                VALUES (?, ?, ?, ?, ?, 'ing-a', 'dlv-a', 'ROUTED')
                """, day, event, organization, "evt_" + event.toString().replace("-", ""),
                Timestamp.from(day.atStartOfDay().toInstant(ZoneOffset.UTC)));
        jdbc.update("""
                INSERT INTO deliveries (accepted_day, id, organization_id, event_id, endpoint_id, status)
                VALUES (?, ?, ?, ?, ?, 'PENDING')
                """, day, delivery, organization, event, endpoint);
        jdbc.update("""
                INSERT INTO delivery_work (delivery_id, accepted_day, organization_id, endpoint_id,
                    status, event_expires_at, next_attempt_at)
                VALUES (?, ?, ?, ?, 'PENDING', clock_timestamp() + interval '90 days', clock_timestamp())
                """, delivery, day, organization, endpoint);
        return delivery;
    }
}

package com.relayforge.deliveryworker.lease;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
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

    private static UUID due(UUID endpoint) {
        UUID organization = UUID.randomUUID();
        UUID event = UUID.randomUUID();
        UUID delivery = UUID.randomUUID();
        LocalDate day = LocalDate.now(ZoneOffset.UTC);
        jdbc.update("""
                INSERT INTO routing_progress (accepted_day, event_id, organization_id,
                    event_accepted_at, ingestion_shard_id, delivery_shard_id, status)
                VALUES (?, ?, ?, ?, 'ing-a', 'dlv-a', 'ROUTED')
                """, day, event, organization, day.atStartOfDay().toInstant(ZoneOffset.UTC));
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

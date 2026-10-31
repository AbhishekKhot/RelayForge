package com.relayforge.outbox;

import java.sql.DriverManager;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcOutboxRepositoryIT {
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.11");
    private static final UUID ORGANIZATION = UUID.fromString("00000000-0000-0000-0000-000000000001");

    static {
        POSTGRES.start();
    }

    private final JdbcTemplate jdbc = new JdbcTemplate(new DriverManagerDataSource(
            POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));

    @BeforeEach
    void schema() {
        for (String owner : List.of("control", "ingestion", "delivery")) {
            jdbc.execute("DROP TABLE IF EXISTS " + owner + "_outbox");
            String aggregateColumn = switch (owner) {
                case "control" -> "aggregate_id varchar(200) NOT NULL";
                case "ingestion" -> "event_id uuid NOT NULL";
                default -> "event_id uuid NOT NULL, delivery_id uuid";
            };
            jdbc.execute("""
                    CREATE TABLE %s_outbox (
                      id uuid PRIMARY KEY, organization_id uuid, %s,
                      message_type varchar(100) NOT NULL, topic varchar(200) NOT NULL,
                      partition_key varchar(200) NOT NULL, payload jsonb NOT NULL,
                      status varchar(20) NOT NULL DEFAULT 'PENDING',
                      created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                      available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                      claimed_by varchar(160), claim_token uuid, claim_expires_at timestamptz,
                      published_at timestamptz, publish_attempts integer NOT NULL DEFAULT 0,
                      last_error_code varchar(80)
                    )
                    """.formatted(owner, aggregateColumn));
        }
    }

    @Test
    void claimsOneDueRowPerWorkerAndSkipsLockedRows() throws Exception {
        UUID first = insertControl("first");
        UUID second = insertControl("second");
        jdbc.update("UPDATE control_outbox SET created_at = clock_timestamp() - interval '1 minute' WHERE id = ?",
                first);
        JdbcOutboxRepository repository = repository(JdbcOutboxRepository.Owner.CONTROL);
        try (var connection = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            connection.setAutoCommit(false);
            try (var statement = connection.prepareStatement("SELECT id FROM control_outbox WHERE id = ? FOR UPDATE")) {
                statement.setObject(1, first);
                statement.executeQuery().close();
            }
            List<ClaimedOutboxRow> skipped = repository.claim("worker-a", 1, Duration.ofSeconds(30));
            assertThat(skipped).extracting(ClaimedOutboxRow::id).containsExactly(second);
            connection.rollback();
        }
        List<ClaimedOutboxRow> recovered = repository.claim("worker-b", 1, Duration.ofSeconds(30));
        assertThat(recovered).extracting(ClaimedOutboxRow::id).containsExactly(first);
        assertThat(recovered.getFirst().publishAttempts()).isEqualTo(1);
    }

    @Test
    void concurrentClaimsNeverReturnTheSameRow() throws Exception {
        UUID id = insertControl("one");
        JdbcOutboxRepository repository = repository(JdbcOutboxRepository.Owner.CONTROL);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch go = new CountDownLatch(1);
        try (var workers = Executors.newFixedThreadPool(2)) {
            var first = workers.submit(() -> claimWhenReleased(repository, ready, go, "worker-a"));
            var second = workers.submit(() -> claimWhenReleased(repository, ready, go, "worker-b"));
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            go.countDown();
            List<ClaimedOutboxRow> combined = new java.util.ArrayList<>(first.get(5, TimeUnit.SECONDS));
            combined.addAll(second.get(5, TimeUnit.SECONDS));
            assertThat(combined).extracting(ClaimedOutboxRow::id).containsExactly(id);
        }
    }

    @Test
    void reclaimsExpiredLeaseAndRejectsStaleAcknowledgement() {
        UUID id = insertControl("leased");
        JdbcOutboxRepository repository = repository(JdbcOutboxRepository.Owner.CONTROL);
        ClaimedOutboxRow old = repository.claim("worker-a", 1, Duration.ofSeconds(5)).getFirst();
        jdbc.update("UPDATE control_outbox SET claim_expires_at = clock_timestamp() - interval '1 second' WHERE id = ?",
                id);
        ClaimedOutboxRow current = repository.claim("worker-b", 1, Duration.ofSeconds(30)).getFirst();
        assertThat(current.claimToken()).isNotEqualTo(old.claimToken());
        assertThat(current.publishAttempts()).isEqualTo(2);
        assertThat(repository.markPublished(id, old.claimToken())).isFalse();
        assertThat(repository.retry(id, old.claimToken(), Duration.ofSeconds(1), "BROKER_TIMEOUT")).isFalse();
        assertThat(repository.markPublished(id, current.claimToken())).isTrue();
        assertThat(repository.markPublished(id, current.claimToken())).isFalse();
        assertThat(jdbc.queryForObject("SELECT status FROM control_outbox WHERE id = ?", String.class, id))
                .isEqualTo("PUBLISHED");
        assertThat(jdbc.queryForObject("SELECT published_at IS NOT NULL FROM control_outbox WHERE id = ?",
                Boolean.class, id)).isTrue();
    }

    @Test
    void retriesAfterDelayAndMapsOwnerSpecificAggregateIds() {
        UUID controlId = insertControl("control-resource");
        UUID ingestionEvent = UUID.randomUUID();
        UUID ingestionId = UUID.randomUUID();
        String locator = "evt_locator";
        jdbc.update("""
                INSERT INTO ingestion_outbox (id, organization_id, event_id, message_type, topic,
                  partition_key, payload) VALUES (?, ?, ?, 'event.accepted.v1', 'events', ?, '{}'::jsonb)
                """, ingestionId, ORGANIZATION, ingestionEvent, locator);
        UUID deliveryId = UUID.randomUUID();
        UUID deliveryRow = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO delivery_outbox (id, organization_id, event_id, delivery_id, message_type,
                  topic, partition_key, payload) VALUES (?, ?, ?, ?, 'delivery.ready.v1', 'deliveries', ?, '{}'::jsonb)
                """, deliveryRow, ORGANIZATION, ingestionEvent, deliveryId, deliveryId.toString());

        JdbcOutboxRepository control = repository(JdbcOutboxRepository.Owner.CONTROL);
        ClaimedOutboxRow row = control.claim("worker", 1, Duration.ofSeconds(30)).getFirst();
        assertThat(row.id()).isEqualTo(controlId);
        assertThat(row.aggregateId()).isEqualTo("control-resource");
        assertThat(control.retry(controlId, row.claimToken(), Duration.ofSeconds(30), "BROKER_TIMEOUT"))
                .isTrue();
        assertThat(control.claim("worker", 1, Duration.ofSeconds(30))).isEmpty();
        assertThat(jdbc.queryForObject("SELECT last_error_code FROM control_outbox WHERE id = ?",
                String.class, controlId)).isEqualTo("BROKER_TIMEOUT");
        jdbc.update("UPDATE control_outbox SET available_at = clock_timestamp() - interval '1 second' WHERE id = ?",
                controlId);
        assertThat(control.claim("worker", 1, Duration.ofSeconds(30)).getFirst().publishAttempts()).isEqualTo(2);
        assertThat(repository(JdbcOutboxRepository.Owner.INGESTION)
                .claim("worker", 1, Duration.ofSeconds(30)).getFirst().aggregateId()).isEqualTo(locator);
        assertThat(repository(JdbcOutboxRepository.Owner.DELIVERY)
                .claim("worker", 1, Duration.ofSeconds(30)).getFirst().aggregateId())
                .isEqualTo(deliveryId.toString());
    }

    @Test
    void rejectsUnboundedClaimsAndUnsafeDiagnosticCodes() {
        JdbcOutboxRepository repository = repository(JdbcOutboxRepository.Owner.CONTROL);
        assertThatThrownBy(() -> repository.claim("worker", 0, Duration.ofSeconds(30)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> repository.claim("worker", 101, Duration.ofSeconds(30)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> repository.claim("worker", 1, Duration.ofDays(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> repository.claim("", 1, Duration.ofSeconds(30)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> repository.retry(UUID.randomUUID(), UUID.randomUUID(),
                Duration.ofSeconds(1), "secret:token"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void doesNotTransferOversizedPayloadToThePublisher() {
        UUID id = UUID.randomUUID();
        String oversized = "{\"body\":\"" + "x".repeat(70000) + "\"}";
        jdbc.update("""
                INSERT INTO control_outbox (id, organization_id, aggregate_id, message_type, topic,
                  partition_key, payload) VALUES (?, ?, 'oversized', 'configuration.changed.v1',
                  'config', ?, ?::jsonb)
                """, id, ORGANIZATION, ORGANIZATION.toString(), oversized);

        ClaimedOutboxRow row = repository(JdbcOutboxRepository.Owner.CONTROL)
                .claim("worker", 1, Duration.ofSeconds(30)).getFirst();
        assertThat(row.id()).isEqualTo(id);
        assertThat(row.payloadJson()).isNull();
    }

    private JdbcOutboxRepository repository(JdbcOutboxRepository.Owner owner) {
        return new JdbcOutboxRepository(jdbc, owner);
    }

    private List<ClaimedOutboxRow> claimWhenReleased(JdbcOutboxRepository repository,
            CountDownLatch ready, CountDownLatch go, String worker) throws InterruptedException {
        ready.countDown();
        if (!go.await(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Claim workers did not start");
        }
        return repository.claim(worker, 1, Duration.ofSeconds(30));
    }

    private UUID insertControl(String aggregateId) {
        UUID id = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO control_outbox (id, organization_id, aggregate_id, message_type, topic,
                  partition_key, payload) VALUES (?, ?, ?, 'configuration.changed.v1', 'config', ?, '{}'::jsonb)
                """, id, ORGANIZATION, aggregateId, ORGANIZATION.toString());
        return id;
    }
}

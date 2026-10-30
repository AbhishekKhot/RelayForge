package com.relayforge.schema;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OwnerMigrationsIT {
    private static final String IMAGE = "postgres:17.11";
    private final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(IMAGE)
            .withDatabaseName("relayforge")
            .withUsername("relayforge")
            .withPassword("relayforge-test");

    @BeforeAll
    void startDatabase() throws Exception {
        postgres.start();
        for (int attempt = 0; attempt < 40; attempt++) {
            try (var connection = DriverManager.getConnection(
                    postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())) {
                return;
            } catch (SQLException unavailable) {
                Thread.sleep(250);
            }
        }
        throw new IllegalStateException("PostgreSQL host port did not become available");
    }

    @AfterAll
    void stopDatabase() {
        postgres.stop();
    }

    @Test
    void controlPlaneMigrationIsIdempotentAndRejectsCrossTenantSubscription() throws Exception {
        migrateTwice("control_plane", "control-plane");
        try (var connection = connection("control_plane")) {
                assertThat(count(connection, "SELECT count(*) FROM flyway_schema_history WHERE success AND version = '1'")).isEqualTo(1);
                assertThat(count(connection, "SELECT count(*) FROM flyway_schema_history WHERE success AND version = '2'")).isEqualTo(1);
                execute(connection, "INSERT INTO organizations (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'one'), ('00000000-0000-0000-0000-000000000002', 'two')");
                execute(connection, "INSERT INTO endpoints (id, organization_id, name, base_url, path, auth_type) VALUES ('00000000-0000-0000-0000-000000000003', '00000000-0000-0000-0000-000000000001', 'first', 'https://example.com', '/', 'NONE')");
                assertThatThrownBy(() -> execute(connection, "INSERT INTO subscriptions (organization_id, endpoint_id, event_type) VALUES ('00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000003', 'order.created')"))
                        .isInstanceOf(SQLException.class);
                execute(connection, "INSERT INTO subscriptions (organization_id, endpoint_id, event_type) VALUES ('00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000003', 'order.created')");
                assertThatThrownBy(() -> execute(connection, "INSERT INTO subscriptions (organization_id, endpoint_id, event_type) VALUES ('00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000003', 'order.created')"))
                        .isInstanceOf(SQLException.class);
                execute(connection, "INSERT INTO api_keys (organization_id, key_id, secret_hash) VALUES ('00000000-0000-0000-0000-000000000001', 'test-key', decode(repeat('aa', 32), 'hex'))");
                execute(connection, "INSERT INTO provisioning_requests (request_hash, organization_id, key_id) VALUES (decode(repeat('bb', 32), 'hex'), '00000000-0000-0000-0000-000000000001', 'test-key')");
                assertThatThrownBy(() -> execute(connection, "INSERT INTO provisioning_requests (request_hash, organization_id, key_id) VALUES (decode(repeat('bb', 32), 'hex'), '00000000-0000-0000-0000-000000000001', 'test-key')"))
                        .isInstanceOf(SQLException.class);
        }
    }

    @Test
    void ingestionMigrationCreatesDayPartitionsAndEnforcesGlobalRegistryKey() throws Exception {
        migrateTwice("ingestion", "ingestion");
        try (var connection = connection("ingestion")) {
                assertThat(count(connection, "SELECT count(*) FROM pg_inherits WHERE inhparent = 'events'::regclass")).isEqualTo(9);
                execute(connection, "INSERT INTO events (accepted_day, id, organization_id, event_type, payload, payload_hash, idempotency_key, accepted_at) VALUES (current_date, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'order.created', '{}'::jsonb, decode(repeat('aa', 32), 'hex'), 'today', now())");
                execute(connection, "INSERT INTO events (accepted_day, id, organization_id, event_type, payload, payload_hash, idempotency_key, accepted_at) VALUES (current_date + 1, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'order.created', '{}'::jsonb, decode(repeat('aa', 32), 'hex'), 'tomorrow', now() + interval '1 day')");
                assertThat(count(connection, "SELECT count(*) FROM events WHERE organization_id = '00000000-0000-0000-0000-000000000002'")).isEqualTo(2);
                assertThatThrownBy(() -> execute(connection, "INSERT INTO events (accepted_day, organization_id, event_type, payload, payload_hash, idempotency_key, accepted_at) VALUES (current_date + 8, '00000000-0000-0000-0000-000000000002', 'order.created', '{}'::jsonb, decode(repeat('aa', 32), 'hex'), 'unprepared', now() + interval '8 days')"))
                        .isInstanceOf(SQLException.class);
                execute(connection, "INSERT INTO idempotency_registry (organization_id, idempotency_key, bucket_id, request_hash, request_hash_version, event_id, event_locator, accepted_day, accepted_at, expires_at) VALUES ('00000000-0000-0000-0000-000000000001', 'same', 0, decode(repeat('aa', 32), 'hex'), 1, gen_random_uuid(), 'v1:one', current_date, now(), now() + interval '2160 hours')");
                assertThatThrownBy(() -> execute(connection, "INSERT INTO idempotency_registry (organization_id, idempotency_key, bucket_id, request_hash, request_hash_version, event_id, event_locator, accepted_day, accepted_at, expires_at) VALUES ('00000000-0000-0000-0000-000000000001', 'same', 0, decode(repeat('aa', 32), 'hex'), 1, gen_random_uuid(), 'v1:two', current_date, now(), now() + interval '2160 hours')"))
                        .isInstanceOf(SQLException.class);
        }
    }

    @Test
    void deliveryMigrationCreatesAllCohortPartitionsAndRejectsForeignDelivery() throws Exception {
        migrateTwice("delivery", "delivery");
        try (var connection = connection("delivery")) {
                assertThat(count(connection, "SELECT count(*) FROM pg_inherits WHERE inhparent = 'deliveries'::regclass")).isEqualTo(9);
                assertThat(count(connection, "SELECT count(*) FROM pg_inherits WHERE inhparent = 'delivery_attempts'::regclass")).isEqualTo(9);
                execute(connection, "INSERT INTO routing_progress (accepted_day, event_id, organization_id, event_accepted_at, ingestion_shard_id, delivery_shard_id, status) VALUES (current_date, '00000000-0000-0000-0000-000000000003', '00000000-0000-0000-0000-000000000001', now(), 'ingestion-1', 'delivery-1', 'ROUTING')");
                execute(connection, "INSERT INTO deliveries (accepted_day, id, organization_id, event_id, endpoint_id) VALUES (current_date, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000003', '00000000-0000-0000-0000-000000000004')");
                assertThatThrownBy(() -> execute(connection, "INSERT INTO delivery_attempts (accepted_day, organization_id, delivery_id, attempt_number, started_at, outcome) VALUES (current_date, '00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000001', 1, now(), 'FAILED')"))
                        .isInstanceOf(SQLException.class);
                execute(connection, "INSERT INTO delivery_attempts (accepted_day, organization_id, delivery_id, attempt_number, started_at, outcome) VALUES (current_date, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000001', 1, now(), 'FAILED')");
                assertThatThrownBy(() -> execute(connection, "INSERT INTO delivery_attempts (accepted_day, organization_id, delivery_id, attempt_number, started_at, outcome) VALUES (current_date, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000001', 1, now(), 'FAILED')"))
                        .isInstanceOf(SQLException.class);
        }
    }

    private void migrateTwice(String schema, String owner) {
        var flyway = Flyway.configure()
                .dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
                .schemas(schema)
                .defaultSchema(schema)
                .locations("filesystem:../relayforge-" + (owner.equals("delivery") ? "delivery-api" : owner) + "/src/main/resources/db/migration")
                .load();
        int expected = switch (owner) {
            case "control-plane" -> 3;
            case "ingestion" -> 2;
            default -> 1;
        };
        assertThat(flyway.migrate().migrationsExecuted).isEqualTo(expected);
        assertThat(flyway.migrate().migrationsExecuted).isZero();
    }

    private Connection connection(String schema) throws SQLException {
        var connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        connection.setSchema(schema);
        return connection;
    }

    private long count(Connection connection, String sql) throws SQLException {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
            result.next();
            return result.getLong(1);
        }
    }

    private void execute(Connection connection, String sql) throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}

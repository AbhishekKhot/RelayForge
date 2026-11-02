package com.relayforge.router.routing;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.contracts.ValidatedEnvelope;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;

class RoutingServiceIT {
    private static final PostgreSQLContainer<?> DATABASE = new PostgreSQLContainer<>("postgres:17.11")
            .withDatabaseName("relayforge_delivery")
            .withUsername("relayforge")
            .withPassword("relayforge-test");
    private static JdbcTemplate jdbc;
    private static RoutingService routing;
    private static TransactionTemplate transactions;

    @BeforeAll
    static void startDatabase() {
        DATABASE.start();
        Flyway.configure().dataSource(DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword())
                .locations("filesystem:../relayforge-delivery-api/src/main/resources/db/migration")
                .load().migrate();
        var dataSource = new DriverManagerDataSource(DATABASE.getJdbcUrl(),
                DATABASE.getUsername(), DATABASE.getPassword());
        jdbc = new JdbcTemplate(dataSource);
        transactions = new TransactionTemplate(new JdbcTransactionManager(dataSource));
        routing = new RoutingService(new JdbcRoutingRepository(jdbc), transactions, 1, 1000);
    }

    @Test
    void bootstrapsProjectionAndAppliesOnlyContiguousRevisions() throws Exception {
        UUID organization = UUID.randomUUID();
        UUID endpoint = UUID.randomUUID();
        UUID subscription = UUID.randomUUID();
        AtomicReference<ConfigurationSnapshot> latest = new AtomicReference<>(new ConfigurationSnapshot(4,
                List.of(new ConfigurationSnapshot.Endpoint(endpoint, "ACTIVE", 1)),
                List.of(new ConfigurationSnapshot.Subscription(subscription, endpoint,
                        "order.created", "ACTIVE", 1, null))));
        ProjectionService projections = new ProjectionService(new JdbcProjectionRepository(jdbc),
                transactions, ignored -> latest.get());

        projections.ensureReady(organization);
        assertThat(jdbc.queryForObject("SELECT source_revision FROM projection_state WHERE organization_id = ?",
                Long.class, organization)).isEqualTo(4);
        assertThat(jdbc.queryForObject("SELECT status FROM endpoint_projection WHERE organization_id = ?",
                String.class, organization)).isEqualTo("ACTIVE");

        projections.applyChange(change(organization, endpoint, subscription, 5, "PAUSED"));
        assertThat(jdbc.queryForObject("SELECT source_revision FROM projection_state WHERE organization_id = ?",
                Long.class, organization)).isEqualTo(5);
        projections.applyChange(change(organization, endpoint, subscription, 5, "ACTIVE"));
        assertThat(jdbc.queryForObject("SELECT status FROM endpoint_projection WHERE organization_id = ?",
                String.class, organization)).isEqualTo("PAUSED");

        latest.set(new ConfigurationSnapshot(7,
                List.of(new ConfigurationSnapshot.Endpoint(endpoint, "DISABLED", 3)),
                List.of(new ConfigurationSnapshot.Subscription(subscription, endpoint,
                        "order.created", "ACTIVE", 1, null))));
        projections.applyChange(change(organization, endpoint, subscription, 7, "DISABLED"));
        assertThat(jdbc.queryForObject("SELECT source_revision FROM projection_state WHERE organization_id = ?",
                Long.class, organization)).isEqualTo(7);
        assertThat(jdbc.queryForObject("SELECT status FROM endpoint_projection WHERE organization_id = ?",
                String.class, organization)).isEqualTo("DISABLED");
    }

    @Test
    void createsTwoDeliveriesWorkRowsAndOutboxRowsOnce() {
        UUID organization = UUID.randomUUID();
        UUID event = UUID.randomUUID();
        UUID first = addSubscription(organization, "order.created");
        UUID second = addSubscription(organization, "order.created");
        jdbc.update("INSERT INTO projection_state (organization_id, source_revision, ready) VALUES (?, 2, true)",
                organization);
        VerifiedEvent accepted = accepted(organization, event, "order.created");

        routing.route(accepted);
        assertThat(count("routing_targets", organization)).isEqualTo(2);
        assertThat(count("deliveries", organization)).isEqualTo(2);
        assertThat(count("delivery_work", organization)).isEqualTo(2);
        assertThat(count("delivery_outbox", organization)).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT status FROM routing_progress WHERE organization_id = ?",
                String.class, organization)).isEqualTo("ROUTED");

        jdbc.update("UPDATE endpoint_projection SET status = 'DISABLED' WHERE organization_id = ? AND endpoint_id = ?",
                organization, second);
        jdbc.update("UPDATE subscription_projection SET status = 'PAUSED' WHERE organization_id = ? AND endpoint_id = ?",
                organization, first);
        routing.route(accepted);
        assertThat(count("deliveries", organization)).isEqualTo(2);
        assertThat(count("delivery_outbox", organization)).isEqualTo(2);
    }

    @Test
    void recordsNoSubscriberOutcomeWithoutCreatingDelivery() {
        UUID organization = UUID.randomUUID();
        jdbc.update("INSERT INTO projection_state (organization_id, source_revision, ready) VALUES (?, 0, true)",
                organization);
        routing.route(accepted(organization, UUID.randomUUID(), "unknown.type"));
        assertThat(count("deliveries", organization)).isZero();
        assertThat(count("delivery_outbox", organization)).isZero();
        assertThat(jdbc.queryForObject("SELECT status FROM routing_progress WHERE organization_id = ?",
                String.class, organization)).isEqualTo("NO_SUBSCRIBERS");
    }

    private static UUID addSubscription(UUID organization, String eventType) {
        UUID endpoint = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO endpoint_projection (organization_id, endpoint_id, status, source_version, updated_at)
                VALUES (?, ?, 'ACTIVE', 1, clock_timestamp())
                """, organization, endpoint);
        jdbc.update("""
                INSERT INTO subscription_projection (organization_id, subscription_id, endpoint_id,
                    event_type, status, source_version, updated_at)
                VALUES (?, ?, ?, ?, 'ACTIVE', 1, clock_timestamp())
                """, organization, UUID.randomUUID(), endpoint, eventType);
        return endpoint;
    }

    private static ValidatedEnvelope change(UUID organization, UUID endpoint, UUID subscription,
            int revision, String status) throws Exception {
        var payload = new ObjectMapper().readTree("""
                {"organization_id":"%s","source_revision":%d,
                 "endpoint":{"endpoint_id":"%s","status":"%s","version":2},
                 "subscriptions":[{"subscription_id":"%s","endpoint_id":"%s",
                   "event_type":"order.created","status":"ACTIVE","version":1}]}
                """.formatted(organization, revision, endpoint, status, subscription, endpoint));
        return new ValidatedEnvelope("msg_1", "configuration.changed.v1", 1,
                Instant.now(), organization, endpoint.toString(), "msg_1", payload);
    }

    private static VerifiedEvent accepted(UUID organization, UUID event, String eventType) {
        LocalDate day = LocalDate.now(ZoneOffset.UTC);
        Instant acceptedAt = day.atStartOfDay().toInstant(ZoneOffset.UTC).plusSeconds(1);
        return new VerifiedEvent(organization, event, day, acceptedAt, eventType, "ing-a", "dlv-a");
    }

    private static int count(String table, UUID organization) {
        return jdbc.queryForObject("SELECT count(*) FROM " + table + " WHERE organization_id = ?",
                Integer.class, organization);
    }
}

package com.relayforge.controlplane.endpoint;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;

public final class JdbcConfigurationSnapshotRepository implements ConfigurationSnapshotRepository {
    private final JdbcTemplate jdbc;

    public JdbcConfigurationSnapshotRepository(JdbcTemplate jdbc) {
        this.jdbc = Objects.requireNonNull(jdbc);
    }

    @Override
    public Optional<Long> revision(UUID organizationId) {
        return jdbc.query("SELECT config_revision FROM organizations WHERE id = ?",
                (row, index) -> row.getLong(1), organizationId).stream().findFirst();
    }

    @Override
    public List<EndpointState> endpoints(UUID organizationId, UUID afterId, int limit) {
        return jdbc.query("""
                SELECT id, status, version FROM endpoints
                WHERE organization_id = ? AND (?::uuid IS NULL OR id > ?::uuid)
                ORDER BY id LIMIT ?
                """, (row, index) -> new EndpointState(row.getObject(1, UUID.class),
                row.getString(2), row.getLong(3)), organizationId, afterId, afterId, limit);
    }

    @Override
    public List<SubscriptionState> subscriptions(UUID organizationId, UUID afterId, int limit) {
        return jdbc.query("""
                SELECT id, endpoint_id, event_type, status, version, filter_config::text
                FROM subscriptions
                WHERE organization_id = ? AND (?::uuid IS NULL OR id > ?::uuid)
                ORDER BY id LIMIT ?
                """, (row, index) -> new SubscriptionState(row.getObject(1, UUID.class),
                row.getObject(2, UUID.class), row.getString(3), row.getString(4), row.getLong(5),
                row.getString(6)), organizationId, afterId, afterId, limit);
    }
}

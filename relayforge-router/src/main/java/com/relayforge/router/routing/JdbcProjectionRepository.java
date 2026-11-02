package com.relayforge.router.routing;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;

public final class JdbcProjectionRepository implements ProjectionRepository {
    private final JdbcTemplate jdbc;

    public JdbcProjectionRepository(JdbcTemplate jdbc) {
        this.jdbc = Objects.requireNonNull(jdbc);
    }

    @Override
    public boolean ready(UUID organizationId) {
        return jdbc.query("SELECT ready FROM projection_state WHERE organization_id = ?",
                (row, index) -> row.getBoolean(1), organizationId)
                .stream().findFirst().orElse(false);
    }

    @Override
    public void replace(UUID organizationId, ConfigurationSnapshot snapshot) {
        ensureState(organizationId);
        State state = lockState(organizationId);
        if (state.revision() > snapshot.revision()) {
            if (state.ready()) {
                return;
            }
            throw new IllegalStateException("Configuration snapshot is stale");
        }
        if (state.ready() && state.revision() == snapshot.revision()) {
            return;
        }
        jdbc.update("DELETE FROM subscription_projection WHERE organization_id = ?", organizationId);
        jdbc.update("DELETE FROM endpoint_projection WHERE organization_id = ?", organizationId);
        for (ConfigurationSnapshot.Endpoint endpoint : snapshot.endpoints()) {
            insertEndpoint(organizationId, endpoint);
        }
        for (ConfigurationSnapshot.Subscription subscription : snapshot.subscriptions()) {
            insertSubscription(organizationId, subscription);
        }
        jdbc.update("""
                UPDATE projection_state SET source_revision = ?, ready = true,
                    updated_at = clock_timestamp() WHERE organization_id = ?
                """, snapshot.revision(), organizationId);
    }

    @Override
    public ApplyResult apply(UUID organizationId, long revision, ConfigurationSnapshot.Endpoint endpoint,
            List<ConfigurationSnapshot.Subscription> subscriptions) {
        ensureState(organizationId);
        State state = lockState(organizationId);
        if (revision <= state.revision()) {
            return ApplyResult.DUPLICATE;
        }
        if (!state.ready() || revision != state.revision() + 1) {
            jdbc.update("UPDATE projection_state SET ready = false WHERE organization_id = ?", organizationId);
            return ApplyResult.GAP;
        }
        int changed = jdbc.update("""
                INSERT INTO endpoint_projection (organization_id, endpoint_id, status,
                    source_version, updated_at)
                VALUES (?, ?, ?, ?, clock_timestamp())
                ON CONFLICT (organization_id, endpoint_id) DO UPDATE SET
                    status = EXCLUDED.status, source_version = EXCLUDED.source_version,
                    updated_at = clock_timestamp()
                WHERE endpoint_projection.source_version <= EXCLUDED.source_version
                """, organizationId, endpoint.id(), endpoint.status(), endpoint.version());
        if (changed != 1) {
            throw new IllegalStateException("Configuration endpoint version went backwards");
        }
        for (ConfigurationSnapshot.Subscription subscription : subscriptions) {
            changed = jdbc.update("""
                    INSERT INTO subscription_projection (organization_id, subscription_id, endpoint_id,
                        event_type, filter_config, status, source_version, updated_at)
                    VALUES (?, ?, ?, ?, ?::jsonb, ?, ?, clock_timestamp())
                    ON CONFLICT (organization_id, subscription_id) DO UPDATE SET
                        endpoint_id = EXCLUDED.endpoint_id, event_type = EXCLUDED.event_type,
                        filter_config = EXCLUDED.filter_config, status = EXCLUDED.status,
                        source_version = EXCLUDED.source_version, updated_at = clock_timestamp()
                    WHERE subscription_projection.source_version <= EXCLUDED.source_version
                    """, organizationId, subscription.id(), subscription.endpointId(),
                    subscription.eventType(), subscription.filterConfig(), subscription.status(),
                    subscription.version());
            if (changed != 1) {
                throw new IllegalStateException("Configuration subscription version went backwards");
            }
        }
        jdbc.update("""
                UPDATE projection_state SET source_revision = ?, ready = true,
                    updated_at = clock_timestamp() WHERE organization_id = ?
                """, revision, organizationId);
        return ApplyResult.APPLIED;
    }

    private void insertEndpoint(UUID organizationId, ConfigurationSnapshot.Endpoint endpoint) {
        jdbc.update("""
                INSERT INTO endpoint_projection (organization_id, endpoint_id, status,
                    source_version, updated_at)
                VALUES (?, ?, ?, ?, clock_timestamp())
                """, organizationId, endpoint.id(), endpoint.status(), endpoint.version());
    }

    private void insertSubscription(UUID organizationId, ConfigurationSnapshot.Subscription subscription) {
        jdbc.update("""
                INSERT INTO subscription_projection (organization_id, subscription_id, endpoint_id,
                    event_type, filter_config, status, source_version, updated_at)
                VALUES (?, ?, ?, ?, ?::jsonb, ?, ?, clock_timestamp())
                """, organizationId, subscription.id(), subscription.endpointId(),
                subscription.eventType(), subscription.filterConfig(), subscription.status(),
                subscription.version());
    }

    private void ensureState(UUID organizationId) {
        jdbc.update("""
                INSERT INTO projection_state (organization_id, source_revision, ready)
                VALUES (?, 0, false) ON CONFLICT (organization_id) DO NOTHING
                """, organizationId);
    }

    private State lockState(UUID organizationId) {
        return jdbc.query("""
                SELECT source_revision, ready FROM projection_state WHERE organization_id = ? FOR UPDATE
                """, (row, index) -> new State(row.getLong(1), row.getBoolean(2)), organizationId)
                .stream().findFirst().orElseThrow(() -> new IllegalStateException("Projection state missing"));
    }

    private record State(long revision, boolean ready) {
    }
}

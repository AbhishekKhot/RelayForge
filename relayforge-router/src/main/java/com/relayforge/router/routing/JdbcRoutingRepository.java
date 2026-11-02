package com.relayforge.router.routing;

import java.sql.Timestamp;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;

public final class JdbcRoutingRepository implements RoutingRepository {
    private final JdbcTemplate jdbc;

    public JdbcRoutingRepository(JdbcTemplate jdbc) {
        this.jdbc = Objects.requireNonNull(jdbc);
    }

    @Override
    public void initialize(VerifiedEvent event, int maxTargets) {
        jdbc.update("""
                INSERT INTO routing_progress (accepted_day, event_id, organization_id, event_accepted_at,
                    ingestion_shard_id, delivery_shard_id, status)
                VALUES (?, ?, ?, ?, ?, ?, 'SELECTING')
                ON CONFLICT (accepted_day, event_id, organization_id) DO NOTHING
                """, event.acceptedDay(), event.eventId(), event.organizationId(),
                Timestamp.from(event.acceptedAt()), event.ingestionShardId(), event.deliveryShardId());
        Progress progress = lockProgress(event);
        if (!progress.ingestionShardId().equals(event.ingestionShardId())
                || !progress.deliveryShardId().equals(event.deliveryShardId())
                || !progress.acceptedAt().equals(event.acceptedAt())) {
            throw new IllegalStateException("Routing reference changed");
        }
        if (!"SELECTING".equals(progress.status())) {
            return;
        }
        if (jdbc.query("SELECT 1 FROM projection_state WHERE organization_id = ? AND ready FOR SHARE",
                (row, index) -> row.getInt(1), event.organizationId()).isEmpty()) {
            throw new IllegalStateException("Configuration projection is not ready");
        }
        Integer filtered = jdbc.queryForObject("""
                SELECT count(*) FROM subscription_projection subscription
                JOIN endpoint_projection endpoint ON endpoint.organization_id = subscription.organization_id
                    AND endpoint.endpoint_id = subscription.endpoint_id
                WHERE subscription.organization_id = ? AND subscription.event_type = ?
                    AND subscription.status = 'ACTIVE' AND endpoint.status = 'ACTIVE'
                    AND subscription.filter_config IS NOT NULL
                """, Integer.class, event.organizationId(), event.eventType());
        if (filtered != null && filtered > 0) {
            throw new IllegalStateException("Subscription filter is not available");
        }
        List<Target> targets = jdbc.query("""
                SELECT subscription.endpoint_id, subscription.subscription_id, subscription.source_version
                FROM subscription_projection subscription
                JOIN endpoint_projection endpoint ON endpoint.organization_id = subscription.organization_id
                    AND endpoint.endpoint_id = subscription.endpoint_id
                WHERE subscription.organization_id = ? AND subscription.event_type = ?
                    AND subscription.status = 'ACTIVE' AND endpoint.status = 'ACTIVE'
                ORDER BY subscription.endpoint_id LIMIT ?
                """, (row, index) -> new Target(row.getObject(1, UUID.class),
                row.getObject(2, UUID.class), row.getLong(3)),
                event.organizationId(), event.eventType(), maxTargets + 1);
        if (targets.size() > maxTargets) {
            throw new IllegalStateException("Routing target limit exceeded");
        }
        for (Target target : targets) {
            jdbc.update("""
                    INSERT INTO routing_targets (accepted_day, event_id, organization_id,
                        endpoint_id, subscription_id, source_version)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT (accepted_day, event_id, organization_id, endpoint_id) DO NOTHING
                    """, event.acceptedDay(), event.eventId(), event.organizationId(),
                    target.endpointId(), target.subscriptionId(), target.sourceVersion());
        }
        jdbc.update("""
                UPDATE routing_progress SET status = ?, target_count = ?, updated_at = clock_timestamp()
                WHERE accepted_day = ? AND event_id = ? AND organization_id = ?
                """, targets.isEmpty() ? "NO_SUBSCRIBERS" : "ROUTING", targets.size(),
                event.acceptedDay(), event.eventId(), event.organizationId());
    }

    @Override
    public int materializeChunk(VerifiedEvent event, int chunkSize) {
        Progress progress = lockProgress(event);
        if ("ROUTED".equals(progress.status()) || "NO_SUBSCRIBERS".equals(progress.status())) {
            return 0;
        }
        if (!"ROUTING".equals(progress.status())) {
            throw new IllegalStateException("Routing targets are not selected");
        }
        List<Target> targets = jdbc.query("""
                SELECT endpoint_id, subscription_id, source_version FROM routing_targets
                WHERE accepted_day = ? AND event_id = ? AND organization_id = ?
                    AND (?::uuid IS NULL OR endpoint_id > ?::uuid)
                ORDER BY endpoint_id LIMIT ?
                """, (row, index) -> new Target(row.getObject(1, UUID.class),
                row.getObject(2, UUID.class), row.getLong(3)), event.acceptedDay(), event.eventId(),
                event.organizationId(), progress.lastEndpointId(), progress.lastEndpointId(), chunkSize);
        for (Target target : targets) {
            List<UUID> inserted = jdbc.query("""
                    INSERT INTO deliveries (accepted_day, organization_id, event_id, endpoint_id,
                        subscription_id, generation, status)
                    VALUES (?, ?, ?, ?, ?, 1, 'PENDING')
                    ON CONFLICT (accepted_day, event_id, endpoint_id, generation) DO NOTHING
                    RETURNING id
                    """, (row, index) -> row.getObject(1, UUID.class), event.acceptedDay(),
                    event.organizationId(), event.eventId(), target.endpointId(), target.subscriptionId());
            if (!inserted.isEmpty()) {
                createWorkAndOutbox(event, target, inserted.getFirst());
            }
        }
        UUID cursor = targets.isEmpty() ? progress.lastEndpointId() : targets.getLast().endpointId();
        jdbc.update("""
                UPDATE routing_progress SET last_endpoint_id = ?, status = ?, updated_at = clock_timestamp()
                WHERE accepted_day = ? AND event_id = ? AND organization_id = ?
                """, cursor, targets.size() < chunkSize ? "ROUTED" : "ROUTING",
                event.acceptedDay(), event.eventId(), event.organizationId());
        return targets.size();
    }

    private void createWorkAndOutbox(VerifiedEvent event, Target target, UUID deliveryId) {
        jdbc.update("""
                INSERT INTO delivery_work (delivery_id, accepted_day, organization_id, endpoint_id,
                    status, event_expires_at, next_attempt_at)
                VALUES (?, ?, ?, ?, 'PENDING', ?, clock_timestamp())
                """, deliveryId, event.acceptedDay(), event.organizationId(), target.endpointId(),
                Timestamp.from(event.acceptedAt().plus(2160, ChronoUnit.HOURS)));
        jdbc.update("""
                INSERT INTO delivery_outbox (organization_id, accepted_day, event_id, delivery_id,
                    message_type, topic, partition_key, payload)
                VALUES (?, ?, ?, ?, 'delivery.ready.v1', 'relayforge.delivery.ready.v1', ?,
                    jsonb_build_object('delivery_id', ?::text))
                """, event.organizationId(), event.acceptedDay(), event.eventId(), deliveryId,
                deliveryId.toString(), deliveryId.toString());
    }

    private Progress lockProgress(VerifiedEvent event) {
        return jdbc.query("""
                SELECT status, last_endpoint_id, event_accepted_at, ingestion_shard_id, delivery_shard_id
                FROM routing_progress WHERE accepted_day = ? AND event_id = ? AND organization_id = ? FOR UPDATE
                """, (row, index) -> new Progress(row.getString(1), row.getObject(2, UUID.class),
                row.getTimestamp(3).toInstant(), row.getString(4), row.getString(5)),
                event.acceptedDay(), event.eventId(), event.organizationId())
                .stream().findFirst().orElseThrow(() -> new IllegalStateException("Routing progress missing"));
    }

    private record Progress(String status, UUID lastEndpointId, java.time.Instant acceptedAt,
            String ingestionShardId, String deliveryShardId) {
    }

    private record Target(UUID endpointId, UUID subscriptionId, long sourceVersion) {
    }
}

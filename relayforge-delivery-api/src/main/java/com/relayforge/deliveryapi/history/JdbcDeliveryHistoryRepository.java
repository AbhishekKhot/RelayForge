package com.relayforge.deliveryapi.history;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;

public final class JdbcDeliveryHistoryRepository implements DeliveryHistoryRepository {
    private final JdbcTemplate jdbc;

    public JdbcDeliveryHistoryRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public List<DeliveryRow> list(UUID organizationId, LocalDate retentionDay,
            Instant retentionCutoff,
            DeliveryQueryParameters.Page page) {
        DeliveryQueryParameters.Cursor cursor = page.cursor();
        return jdbc.query("""
                SELECT delivery.accepted_day, delivery.id, progress.event_locator,
                    delivery.endpoint_id, delivery.subscription_id, delivery.generation,
                    delivery.status, delivery.attempt_count, delivery.claim_count,
                    delivery.next_attempt_at, delivery.last_http_status,
                    delivery.last_error_code, delivery.created_at, delivery.updated_at,
                    delivery.completed_at
                FROM deliveries delivery JOIN routing_progress progress
                    ON progress.accepted_day = delivery.accepted_day
                    AND progress.event_id = delivery.event_id
                    AND progress.organization_id = delivery.organization_id
                WHERE delivery.organization_id = ? AND delivery.accepted_day >= ?
                    AND progress.event_accepted_at > ?
                    AND delivery.created_at >= ? AND delivery.created_at < ?
                    AND (?::varchar IS NULL OR delivery.status = ?)
                    AND (?::uuid IS NULL OR delivery.endpoint_id = ?::uuid)
                    AND (?::timestamptz IS NULL OR
                        (delivery.created_at, delivery.id) < (?::timestamptz, ?::uuid))
                ORDER BY delivery.created_at DESC, delivery.id DESC LIMIT ?
                """, (row, index) -> mapDelivery(row), organizationId, retentionDay,
                Timestamp.from(retentionCutoff),
                Timestamp.from(page.from()), Timestamp.from(page.to()),
                page.status(), page.status(), page.endpointId(), page.endpointId(),
                cursor == null ? null : Timestamp.from(cursor.createdAt()),
                cursor == null ? null : Timestamp.from(cursor.createdAt()),
                cursor == null ? null : cursor.deliveryId(), page.limit() + 1);
    }

    @Override
    public Optional<DeliveryRow> find(UUID organizationId, UUID deliveryId,
            LocalDate retentionDay, Instant retentionCutoff) {
        return jdbc.query("""
                SELECT delivery.accepted_day, delivery.id, progress.event_locator,
                    delivery.endpoint_id, delivery.subscription_id, delivery.generation,
                    delivery.status, delivery.attempt_count, delivery.claim_count,
                    delivery.next_attempt_at, delivery.last_http_status,
                    delivery.last_error_code, delivery.created_at, delivery.updated_at,
                    delivery.completed_at
                FROM deliveries delivery JOIN routing_progress progress
                    ON progress.accepted_day = delivery.accepted_day
                    AND progress.event_id = delivery.event_id
                    AND progress.organization_id = delivery.organization_id
                WHERE delivery.organization_id = ? AND delivery.id = ?
                    AND delivery.accepted_day >= ?
                    AND progress.event_accepted_at > ?
                ORDER BY delivery.accepted_day DESC LIMIT 1
                """, (row, index) -> mapDelivery(row), organizationId, deliveryId,
                retentionDay, Timestamp.from(retentionCutoff))
                .stream().findFirst();
    }

    @Override
    public List<AttemptRow> attempts(UUID organizationId, LocalDate acceptedDay, UUID deliveryId) {
        return jdbc.query("""
                SELECT attempt_number, started_at, finished_at, outcome, http_status,
                    duration_ms, error_code, response_excerpt
                FROM delivery_attempts
                WHERE organization_id = ? AND accepted_day = ? AND delivery_id = ?
                ORDER BY attempt_number ASC LIMIT 20
                """, (row, index) -> new AttemptRow(row.getInt(1), row.getTimestamp(2).toInstant(),
                instant(row, 3), row.getString(4), row.getObject(5, Integer.class),
                row.getObject(6, Integer.class), row.getString(7), row.getString(8)),
                organizationId, acceptedDay, deliveryId);
    }

    private DeliveryRow mapDelivery(ResultSet row) throws SQLException {
        return new DeliveryRow(row.getObject(1, LocalDate.class), row.getObject(2, UUID.class),
                row.getString(3), row.getObject(4, UUID.class), row.getObject(5, UUID.class),
                row.getInt(6), row.getString(7), row.getInt(8), row.getInt(9),
                instant(row, 10), row.getObject(11, Integer.class), row.getString(12),
                row.getTimestamp(13).toInstant(), row.getTimestamp(14).toInstant(), instant(row, 15));
    }

    private java.time.Instant instant(ResultSet row, int index) throws SQLException {
        Timestamp value = row.getTimestamp(index);
        return value == null ? null : value.toInstant();
    }
}

package com.relayforge.deliveryapi.summary;

import java.time.LocalDate;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;

public final class JdbcDeliverySummaryRepository implements DeliverySummaryRepository {
    private final JdbcTemplate jdbc;

    public JdbcDeliverySummaryRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public Optional<DeliverySummary> find(UUID organizationId, LocalDate acceptedDay, UUID eventId) {
        return jdbc.query("""
                SELECT progress.status, progress.target_count, count(delivery.id),
                    count(delivery.id) FILTER (WHERE delivery.status = 'SUCCEEDED'),
                    count(delivery.id) FILTER (WHERE delivery.status = 'DEAD_LETTERED')
                FROM routing_progress progress
                LEFT JOIN deliveries delivery ON delivery.accepted_day = progress.accepted_day
                    AND delivery.event_id = progress.event_id
                    AND delivery.organization_id = progress.organization_id
                WHERE progress.organization_id = ? AND progress.accepted_day = ? AND progress.event_id = ?
                GROUP BY progress.status, progress.target_count
                """, (row, index) -> new DeliverySummary(row.getString(1), row.getInt(2),
                row.getInt(3), row.getInt(4), row.getInt(5)), organizationId, acceptedDay, eventId)
                .stream().findFirst();
    }
}

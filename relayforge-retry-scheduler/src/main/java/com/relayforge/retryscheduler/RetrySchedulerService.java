package com.relayforge.retryscheduler;

import java.time.LocalDate;
import java.util.Objects;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

public final class RetrySchedulerService {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;

    public RetrySchedulerService(JdbcTemplate jdbc, TransactionTemplate transactions) {
        this.jdbc = Objects.requireNonNull(jdbc);
        this.transactions = Objects.requireNonNull(transactions);
    }

    public int scanDue(int limit) {
        if (limit < 1 || limit > 100) {
            throw new IllegalArgumentException("Invalid retry batch size");
        }
        return Objects.requireNonNull(transactions.execute(status -> {
            var rows = jdbc.query("""
                    SELECT work.delivery_id, work.accepted_day, work.organization_id,
                        work.event_expires_at <= clock_timestamp()
                    FROM delivery_work work
                    WHERE work.status = 'RETRY_SCHEDULED'
                        AND work.next_attempt_at <= clock_timestamp()
                    ORDER BY work.next_attempt_at, work.delivery_id
                    LIMIT ? FOR UPDATE SKIP LOCKED
                    """, (row, index) -> new Due(row.getObject(1, UUID.class),
                    row.getObject(2, LocalDate.class), row.getObject(3, UUID.class),
                    row.getBoolean(4)), limit);
            int enqueued = 0;
            for (Due row : rows) {
                var delivery = jdbc.query("""
                        SELECT event_id, status FROM deliveries
                        WHERE accepted_day = ? AND id = ? AND organization_id = ? FOR UPDATE
                        """, (result, index) -> new Delivery(result.getObject(1, UUID.class),
                        result.getString(2)), row.day(), row.deliveryId(), row.organizationId());
                if (delivery.size() != 1 || !"RETRY_SCHEDULED".equals(delivery.getFirst().status())) {
                    throw new IllegalStateException("Delivery and retry work state disagree");
                }
                if (row.expired()) {
                    expire(row);
                } else {
                    enqueue(row, delivery.getFirst().eventId());
                    enqueued++;
                }
            }
            return enqueued;
        }));
    }

    private void expire(Due row) {
        int delivery = jdbc.update("""
                UPDATE deliveries SET status = 'DEAD_LETTERED',
                    last_error_code = 'EVENT_EXPIRED', completed_at = clock_timestamp(),
                    updated_at = clock_timestamp(), version = version + 1
                WHERE accepted_day = ? AND id = ? AND organization_id = ?
                    AND status = 'RETRY_SCHEDULED'
                """, row.day(), row.deliveryId(), row.organizationId());
        int work = jdbc.update("""
                DELETE FROM delivery_work WHERE delivery_id = ? AND status = 'RETRY_SCHEDULED'
                """, row.deliveryId());
        requirePair(delivery, work);
    }

    private void enqueue(Due row, UUID eventId) {
        int work = jdbc.update("""
                UPDATE delivery_work SET status = 'ENQUEUED', updated_at = clock_timestamp()
                WHERE delivery_id = ? AND status = 'RETRY_SCHEDULED'
                """, row.deliveryId());
        int delivery = jdbc.update("""
                UPDATE deliveries SET status = 'ENQUEUED', updated_at = clock_timestamp(),
                    version = version + 1
                WHERE accepted_day = ? AND id = ? AND organization_id = ?
                    AND status = 'RETRY_SCHEDULED'
                """, row.day(), row.deliveryId(), row.organizationId());
        requirePair(delivery, work);
        jdbc.update("""
                INSERT INTO delivery_outbox (organization_id, accepted_day, event_id,
                    delivery_id, message_type, topic, partition_key, payload)
                VALUES (?, ?, ?, ?, 'delivery.ready.v1', 'relayforge.delivery.ready.v1', ?,
                    jsonb_build_object('delivery_id', ?::text))
                """, row.organizationId(), row.day(), eventId, row.deliveryId(),
                row.deliveryId().toString(), row.deliveryId().toString());
    }

    private void requirePair(int delivery, int work) {
        if (delivery != 1 || work != 1) {
            throw new IllegalStateException("Retry transition did not update both rows");
        }
    }

    private record Due(UUID deliveryId, LocalDate day, UUID organizationId, boolean expired) {
    }

    private record Delivery(UUID eventId, String status) {
    }
}

package com.relayforge.deliveryworker.lease;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

public final class DeliveryAttemptService {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;

    public DeliveryAttemptService(JdbcTemplate jdbc, TransactionTemplate transactions) {
        this.jdbc = Objects.requireNonNull(jdbc);
        this.transactions = Objects.requireNonNull(transactions);
    }

    public WorkReference candidate(UUID deliveryId) {
        Objects.requireNonNull(deliveryId);
        return jdbc.query("""
                SELECT work.organization_id, work.endpoint_id, delivery.attempt_count,
                    delivery.created_at, work.event_expires_at
                FROM delivery_work work JOIN deliveries delivery
                    ON delivery.accepted_day = work.accepted_day AND delivery.id = work.delivery_id
                    AND delivery.organization_id = work.organization_id
                WHERE work.delivery_id = ?
                    AND work.status IN ('PENDING', 'ENQUEUED', 'RETRY_SCHEDULED')
                    AND work.next_attempt_at <= clock_timestamp()
                """, (row, index) -> new WorkReference(row.getObject(1, UUID.class),
                row.getObject(2, UUID.class), row.getInt(3),
                row.getTimestamp(4).toInstant(), row.getTimestamp(5).toInstant()), deliveryId)
                .stream().findFirst().orElse(null);
    }

    public void finishExhausted(UUID deliveryId, int maxAttempts) {
        if (deliveryId == null || maxAttempts < 1 || maxAttempts > 20) {
            throw new IllegalArgumentException("Invalid attempt limit");
        }
        transactions.executeWithoutResult(status -> {
            var rows = jdbc.query("""
                    SELECT work.accepted_day, work.organization_id, delivery.attempt_count
                    FROM delivery_work work JOIN deliveries delivery
                        ON delivery.accepted_day = work.accepted_day AND delivery.id = work.delivery_id
                        AND delivery.organization_id = work.organization_id
                    WHERE work.delivery_id = ?
                        AND work.status IN ('PENDING', 'ENQUEUED', 'RETRY_SCHEDULED')
                    FOR UPDATE OF work, delivery
                    """, (row, index) -> new Exhausted(row.getObject(1, java.time.LocalDate.class),
                    row.getObject(2, UUID.class), row.getInt(3)), deliveryId);
            if (rows.isEmpty() || rows.getFirst().attemptCount() < maxAttempts) {
                return;
            }
            Exhausted row = rows.getFirst();
            jdbc.update("""
                    UPDATE deliveries SET status = 'DEAD_LETTERED', last_error_code = 'MAX_ATTEMPTS',
                        completed_at = clock_timestamp(), updated_at = clock_timestamp(),
                        version = version + 1
                    WHERE accepted_day = ? AND id = ? AND organization_id = ?
                    """, row.day(), deliveryId, row.organizationId());
            jdbc.update("DELETE FROM delivery_work WHERE delivery_id = ?", deliveryId);
        });
    }

    public boolean rejectUnclaimed(UUID deliveryId, String errorCode) {
        Objects.requireNonNull(deliveryId);
        validateErrorCode(errorCode);
        return Boolean.TRUE.equals(transactions.execute(status -> {
            var rows = jdbc.query("""
                    SELECT work.accepted_day, work.organization_id
                    FROM delivery_work work JOIN deliveries delivery
                        ON delivery.accepted_day = work.accepted_day AND delivery.id = work.delivery_id
                        AND delivery.organization_id = work.organization_id
                    WHERE work.delivery_id = ?
                        AND work.status IN ('PENDING', 'ENQUEUED', 'RETRY_SCHEDULED')
                    FOR UPDATE OF work, delivery
                    """, (row, index) -> new Rejected(row.getObject(1, java.time.LocalDate.class),
                    row.getObject(2, UUID.class)), deliveryId);
            if (rows.size() != 1) {
                return false;
            }
            markRejected(rows.getFirst(), deliveryId, errorCode);
            jdbc.update("DELETE FROM delivery_work WHERE delivery_id = ?", deliveryId);
            return true;
        }));
    }

    public boolean rejectClaimed(DeliveryClaimService.Lease lease, String errorCode) {
        Objects.requireNonNull(lease);
        validateErrorCode(errorCode);
        return Boolean.TRUE.equals(transactions.execute(status -> {
            var rows = jdbc.query("""
                    SELECT work.accepted_day, work.organization_id
                    FROM delivery_work work JOIN deliveries delivery
                        ON delivery.accepted_day = work.accepted_day AND delivery.id = work.delivery_id
                        AND delivery.organization_id = work.organization_id
                    WHERE work.delivery_id = ? AND work.organization_id = ?
                        AND work.status = 'IN_PROGRESS' AND work.lease_token = ?
                        AND work.lease_owner = ? AND work.lease_expires_at > clock_timestamp()
                        AND work.processing_deadline_at > clock_timestamp()
                        AND delivery.status = 'IN_PROGRESS' AND delivery.lease_token = ?
                        AND delivery.version = ?
                    FOR UPDATE OF work, delivery
                    """, (row, index) -> new Rejected(row.getObject(1, java.time.LocalDate.class),
                    row.getObject(2, UUID.class)), lease.deliveryId(), lease.organizationId(),
                    lease.token(), lease.workerId(), lease.token(), lease.version());
            if (rows.size() != 1) {
                return false;
            }
            markRejected(rows.getFirst(), lease.deliveryId(), errorCode);
            jdbc.update("DELETE FROM delivery_work WHERE delivery_id = ? AND lease_token = ?",
                    lease.deliveryId(), lease.token());
            return true;
        }));
    }

    private void markRejected(Rejected row, UUID deliveryId, String errorCode) {
        jdbc.update("""
                UPDATE deliveries SET status = 'DEAD_LETTERED', last_error_code = ?,
                    lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
                    processing_deadline_at = NULL, completed_at = clock_timestamp(),
                    updated_at = clock_timestamp(), version = version + 1
                WHERE accepted_day = ? AND id = ? AND organization_id = ?
                """, errorCode, row.day(), deliveryId, row.organizationId());
    }

    private void validateErrorCode(String errorCode) {
        if (errorCode == null || !errorCode.matches("[A-Z_]{1,80}")) {
            throw new IllegalArgumentException("Invalid delivery error code");
        }
    }

    public void deferUnclaimed(UUID deliveryId, Instant retryAt) {
        Objects.requireNonNull(deliveryId);
        Objects.requireNonNull(retryAt);
        transactions.executeWithoutResult(status -> {
            var rows = jdbc.query("""
                    SELECT work.accepted_day, work.organization_id, delivery.event_id
                    FROM delivery_work work JOIN deliveries delivery
                        ON delivery.accepted_day = work.accepted_day AND delivery.id = work.delivery_id
                        AND delivery.organization_id = work.organization_id
                    WHERE work.delivery_id = ?
                        AND work.status IN ('PENDING', 'ENQUEUED', 'RETRY_SCHEDULED')
                    FOR UPDATE OF work, delivery
                    """, (row, index) -> new Deferred(row.getObject(1, java.time.LocalDate.class),
                    row.getObject(2, UUID.class), row.getObject(3, UUID.class)), deliveryId);
            if (rows.isEmpty()) {
                return;
            }
            Deferred row = rows.getFirst();
            jdbc.update("UPDATE delivery_work SET next_attempt_at = ?, updated_at = clock_timestamp() "
                    + "WHERE delivery_id = ?", Timestamp.from(retryAt), deliveryId);
            jdbc.update("UPDATE deliveries SET next_attempt_at = ?, updated_at = clock_timestamp() "
                    + "WHERE accepted_day = ? AND id = ? AND organization_id = ?",
                    Timestamp.from(retryAt), row.day(), deliveryId, row.organizationId());
            jdbc.update("""
                    INSERT INTO delivery_outbox (organization_id, accepted_day, event_id,
                        delivery_id, message_type, topic, partition_key, payload, available_at)
                    VALUES (?, ?, ?, ?, 'delivery.ready.v1', 'relayforge.delivery.ready.v1', ?,
                        jsonb_build_object('delivery_id', ?::text), ?)
                    """, row.organizationId(), row.day(), row.eventId(), deliveryId,
                    deliveryId.toString(), deliveryId.toString(), Timestamp.from(retryAt));
        });
    }

    public String eventLocator(DeliveryClaimService.Lease lease) {
        Objects.requireNonNull(lease);
        var rows = jdbc.query("""
                SELECT progress.event_locator FROM deliveries delivery
                JOIN routing_progress progress ON progress.accepted_day = delivery.accepted_day
                    AND progress.event_id = delivery.event_id
                    AND progress.organization_id = delivery.organization_id
                WHERE delivery.accepted_day = ? AND delivery.id = ?
                    AND delivery.organization_id = ? AND delivery.endpoint_id = ?
                """, (row, index) -> row.getString(1), lease.acceptedDay(),
                lease.deliveryId(), lease.organizationId(), lease.endpointId());
        if (rows.size() != 1 || rows.getFirst() == null) {
            throw new IllegalStateException("Delivery event locator is unavailable");
        }
        return rows.getFirst();
    }

    public Attempt reserve(DeliveryClaimService.Lease lease, int maxAttempts) {
        Objects.requireNonNull(lease);
        if (maxAttempts < 1 || maxAttempts > 20) {
            throw new IllegalArgumentException("Invalid attempt limit");
        }
        return transactions.execute(status -> {
            var rows = jdbc.query("""
                    SELECT delivery.event_id, progress.event_locator, delivery.attempt_count
                    FROM delivery_work work JOIN deliveries delivery
                        ON delivery.accepted_day = work.accepted_day AND delivery.id = work.delivery_id
                        AND delivery.organization_id = work.organization_id
                    JOIN routing_progress progress ON progress.accepted_day = delivery.accepted_day
                        AND progress.event_id = delivery.event_id
                        AND progress.organization_id = delivery.organization_id
                    WHERE work.delivery_id = ? AND work.organization_id = ?
                        AND work.status = 'IN_PROGRESS' AND work.lease_token = ?
                        AND work.lease_owner = ? AND work.lease_expires_at > clock_timestamp()
                        AND work.processing_deadline_at > clock_timestamp()
                        AND delivery.status = 'IN_PROGRESS' AND delivery.lease_token = ?
                        AND delivery.version = ? AND delivery.endpoint_id = ?
                    FOR UPDATE OF work, delivery
                    """, (row, index) -> new Reservation(row.getObject(1, UUID.class),
                    row.getString(2), row.getInt(3)), lease.deliveryId(), lease.organizationId(),
                    lease.token(), lease.workerId(), lease.token(), lease.version(), lease.endpointId());
            if (rows.size() != 1 || rows.getFirst().locator() == null) {
                throw new IllegalStateException("Delivery lease or event locator is unavailable");
            }
            Reservation row = rows.getFirst();
            int number = row.count() + 1;
            if (number > maxAttempts) {
                throw new IllegalStateException("Delivery attempt limit reached");
            }
            jdbc.update("""
                    UPDATE deliveries SET attempt_count = ?, updated_at = clock_timestamp()
                    WHERE accepted_day = ? AND id = ? AND organization_id = ?
                    """, number, lease.acceptedDay(), lease.deliveryId(), lease.organizationId());
            UUID attemptId = UUID.randomUUID();
            jdbc.update("""
                    INSERT INTO delivery_attempts (accepted_day, id, organization_id, delivery_id,
                        attempt_number, started_at, outcome)
                    VALUES (?, ?, ?, ?, ?, clock_timestamp(), 'IN_PROGRESS')
                    """, lease.acceptedDay(), attemptId, lease.organizationId(),
                    lease.deliveryId(), number);
            return new Attempt(attemptId, row.eventId(), row.locator(), number);
        });
    }

    public boolean recordAuthentication(DeliveryClaimService.Lease lease, String outcome,
            Integer httpStatus, String errorCode, Instant startedAt) {
        Objects.requireNonNull(lease);
        Objects.requireNonNull(startedAt);
        if (outcome == null || !outcome.matches("[A-Z_]{1,32}")
                || httpStatus != null && (httpStatus < 100 || httpStatus > 599)
                || errorCode != null && !errorCode.matches("[A-Z_]{1,80}")) {
            throw new IllegalArgumentException("Invalid authentication outcome");
        }
        return Boolean.TRUE.equals(transactions.execute(status -> {
            var owned = jdbc.query("""
                    SELECT work.delivery_id FROM delivery_work work
                    WHERE work.delivery_id = ? AND work.accepted_day = ?
                        AND work.organization_id = ? AND work.endpoint_id = ?
                        AND work.status = 'IN_PROGRESS' AND work.lease_token = ?
                        AND work.lease_owner = ? AND work.lease_expires_at > clock_timestamp()
                        AND work.processing_deadline_at > clock_timestamp()
                    FOR UPDATE
                    """, (row, index) -> row.getObject(1, UUID.class), lease.deliveryId(),
                    lease.acceptedDay(), lease.organizationId(), lease.endpointId(),
                    lease.token(), lease.workerId());
            if (owned.isEmpty()) {
                return false;
            }
            int number = jdbc.queryForObject("""
                    SELECT coalesce(max(attempt_number), 0) + 1 FROM authentication_attempts
                    WHERE accepted_day = ? AND organization_id = ? AND delivery_id = ?
                    """, Integer.class, lease.acceptedDay(), lease.organizationId(), lease.deliveryId());
            jdbc.update("""
                    INSERT INTO authentication_attempts (accepted_day, organization_id, endpoint_id,
                        delivery_id, attempt_number, started_at, finished_at, outcome,
                        http_status, duration_ms, error_code)
                    VALUES (?, ?, ?, ?, ?, ?, clock_timestamp(), ?, ?,
                        LEAST(2147483647, GREATEST(0,
                            EXTRACT(EPOCH FROM (clock_timestamp() - ?::timestamptz)) * 1000)::bigint), ?)
                    """, lease.acceptedDay(), lease.organizationId(), lease.endpointId(),
                    lease.deliveryId(), number, Timestamp.from(startedAt), outcome,
                    httpStatus, Timestamp.from(startedAt), errorCode);
            return true;
        }));
    }

    public boolean deferClaimed(DeliveryClaimService.Lease lease, Instant nextAttemptAt,
            String errorCode) {
        Objects.requireNonNull(lease);
        Objects.requireNonNull(nextAttemptAt);
        validateErrorCode(errorCode);
        return Boolean.TRUE.equals(transactions.execute(status -> {
            var owned = jdbc.query("""
                    SELECT work.delivery_id FROM delivery_work work JOIN deliveries delivery
                        ON delivery.accepted_day = work.accepted_day AND delivery.id = work.delivery_id
                        AND delivery.organization_id = work.organization_id
                    WHERE work.delivery_id = ? AND work.accepted_day = ?
                        AND work.organization_id = ? AND work.status = 'IN_PROGRESS'
                        AND work.lease_token = ? AND work.lease_owner = ?
                        AND work.lease_expires_at > clock_timestamp()
                        AND work.processing_deadline_at > clock_timestamp()
                        AND delivery.status = 'IN_PROGRESS' AND delivery.lease_token = ?
                        AND delivery.version = ?
                    FOR UPDATE OF work, delivery
                    """, (row, index) -> row.getObject(1, UUID.class), lease.deliveryId(),
                    lease.acceptedDay(), lease.organizationId(), lease.token(), lease.workerId(),
                    lease.token(), lease.version());
            if (owned.isEmpty()) {
                return false;
            }
            jdbc.update("""
                    UPDATE deliveries SET status = 'RETRY_SCHEDULED', last_error_code = ?,
                        next_attempt_at = ?, lease_owner = NULL, lease_token = NULL,
                        lease_expires_at = NULL, processing_deadline_at = NULL,
                        updated_at = clock_timestamp(), version = version + 1
                    WHERE accepted_day = ? AND id = ? AND organization_id = ? AND lease_token = ?
                    """, errorCode, Timestamp.from(nextAttemptAt), lease.acceptedDay(),
                    lease.deliveryId(), lease.organizationId(), lease.token());
            jdbc.update("""
                    UPDATE delivery_work SET status = 'RETRY_SCHEDULED', next_attempt_at = ?,
                        lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
                        processing_deadline_at = NULL, updated_at = clock_timestamp()
                    WHERE delivery_id = ? AND lease_token = ?
                    """, Timestamp.from(nextAttemptAt), lease.deliveryId(), lease.token());
            return true;
        }));
    }

    public boolean finish(DeliveryClaimService.Lease lease, Attempt attempt, int httpStatus,
            String errorCode, String responseExcerpt, Instant nextAttemptAt) {
        Objects.requireNonNull(lease);
        Objects.requireNonNull(attempt);
        if (httpStatus < 0 || httpStatus > 599 || errorCode != null
                && !errorCode.matches("[A-Z_]{1,80}") || responseExcerpt != null
                && (responseExcerpt.length() > 1000 || responseExcerpt.codePoints().anyMatch(value ->
                value < 0x20 || value > 0x7e))) {
            throw new IllegalArgumentException("Invalid delivery outcome");
        }
        return Boolean.TRUE.equals(transactions.execute(status -> {
            var owned = jdbc.query("""
                    SELECT delivery.version FROM delivery_work work JOIN deliveries delivery
                        ON delivery.accepted_day = work.accepted_day AND delivery.id = work.delivery_id
                    WHERE work.delivery_id = ? AND work.organization_id = ?
                        AND work.status = 'IN_PROGRESS' AND work.lease_token = ?
                        AND work.lease_owner = ? AND work.lease_expires_at > clock_timestamp()
                        AND work.processing_deadline_at > clock_timestamp()
                        AND delivery.status = 'IN_PROGRESS' AND delivery.lease_token = ?
                        AND delivery.version = ?
                    FOR UPDATE OF work, delivery
                    """, (row, index) -> row.getLong(1), lease.deliveryId(), lease.organizationId(),
                    lease.token(), lease.workerId(), lease.token(), lease.version());
            if (owned.size() != 1) {
                return false;
            }
            boolean success = httpStatus >= 200 && httpStatus < 300;
            boolean retry = !success && nextAttemptAt != null;
            String outcome = success ? "SUCCEEDED" : retry ? "RETRY_SCHEDULED" : "DEAD_LETTERED";
            int updated = jdbc.update("""
                    UPDATE delivery_attempts SET finished_at = clock_timestamp(), outcome = ?,
                        http_status = ?, duration_ms = LEAST(2147483647,
                            GREATEST(0, EXTRACT(EPOCH FROM (clock_timestamp() - started_at)) * 1000)::bigint),
                        error_code = ?, response_excerpt = ?
                    WHERE accepted_day = ? AND id = ? AND organization_id = ?
                        AND delivery_id = ? AND outcome = 'IN_PROGRESS'
                    """, outcome, httpStatus == 0 ? null : httpStatus, errorCode, responseExcerpt,
                    lease.acceptedDay(), attempt.id(), lease.organizationId(), lease.deliveryId());
            if (updated != 1) {
                throw new IllegalStateException("Delivery attempt is unavailable");
            }
            jdbc.update("""
                    UPDATE deliveries SET status = ?, last_http_status = ?, last_error_code = ?,
                        lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
                        processing_deadline_at = NULL, next_attempt_at = COALESCE(?, next_attempt_at),
                        completed_at = CASE WHEN ? THEN clock_timestamp() ELSE NULL END,
                        updated_at = clock_timestamp(), version = version + 1
                    WHERE accepted_day = ? AND id = ? AND organization_id = ? AND lease_token = ?
                    """, outcome, httpStatus == 0 ? null : httpStatus, errorCode,
                    nextAttemptAt == null ? null : Timestamp.from(nextAttemptAt), !retry,
                    lease.acceptedDay(), lease.deliveryId(), lease.organizationId(), lease.token());
            if (retry) {
                jdbc.update("""
                        UPDATE delivery_work SET status = 'RETRY_SCHEDULED', lease_owner = NULL,
                            lease_token = NULL, lease_expires_at = NULL, processing_deadline_at = NULL,
                            next_attempt_at = ?, updated_at = clock_timestamp()
                        WHERE delivery_id = ? AND lease_token = ?
                        """, Timestamp.from(nextAttemptAt), lease.deliveryId(), lease.token());
            } else {
                jdbc.update("DELETE FROM delivery_work WHERE delivery_id = ? AND lease_token = ?",
                        lease.deliveryId(), lease.token());
            }
            return true;
        }));
    }

    private record Reservation(UUID eventId, String locator, int count) {
    }

    private record Deferred(java.time.LocalDate day, UUID organizationId, UUID eventId) {
    }

    public record Attempt(UUID id, UUID eventId, String locator, int number) {
    }

    public record WorkReference(UUID organizationId, UUID endpointId, int attemptCount,
            Instant createdAt, Instant eventExpiresAt) {
    }

    private record Exhausted(java.time.LocalDate day, UUID organizationId, int attemptCount) {
    }

    private record Rejected(java.time.LocalDate day, UUID organizationId) {
    }
}

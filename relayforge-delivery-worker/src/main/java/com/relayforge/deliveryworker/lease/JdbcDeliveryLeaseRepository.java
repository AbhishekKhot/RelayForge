package com.relayforge.deliveryworker.lease;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import org.springframework.jdbc.core.JdbcTemplate;

public final class JdbcDeliveryLeaseRepository implements DeliveryLeaseRepository {
    private static final long CLAIM_LOCK_KEY = 763426845L;
    private final JdbcTemplate jdbc;
    private final Supplier<UUID> tokens;

    public JdbcDeliveryLeaseRepository(JdbcTemplate jdbc) {
        this(jdbc, UUID::randomUUID);
    }

    public JdbcDeliveryLeaseRepository(JdbcTemplate jdbc, Supplier<UUID> tokens) {
        this.jdbc = Objects.requireNonNull(jdbc);
        this.tokens = Objects.requireNonNull(tokens);
    }

    @Override
    public Optional<DeliveryClaimService.Lease> claim(UUID deliveryId, String workerId,
            int endpointLimit, int organizationLimit, int platformLimit) {
        jdbc.query("SELECT pg_advisory_xact_lock(?)", result -> {
            result.next();
            return null;
        }, CLAIM_LOCK_KEY);
        Instant now = databaseTime();
        List<Work> rows = jdbc.query("""
                SELECT accepted_day, organization_id, endpoint_id, lease_token
                FROM delivery_work
                WHERE delivery_id = ? AND status IN ('PENDING', 'ENQUEUED', 'RETRY_SCHEDULED')
                    AND next_attempt_at <= ? AND event_expires_at > ?
                FOR UPDATE SKIP LOCKED
                """, (row, index) -> new Work(row.getObject(1, LocalDate.class),
                row.getObject(2, UUID.class), row.getObject(3, UUID.class),
                row.getObject(4, UUID.class)), deliveryId, Timestamp.from(now), Timestamp.from(now));
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        Work work = rows.getFirst();
        if (work.token() != null) {
            throw new IllegalStateException("Eligible work retains a lease token");
        }
        if (activeCount(null, null, now) >= platformLimit
                || activeCount(work.endpointId(), null, now) >= endpointLimit
                || activeCount(null, work.organizationId(), now) >= organizationLimit) {
            return Optional.empty();
        }
        Delivery delivery = lockDelivery(work.day(), deliveryId);
        if (!delivery.status().equals("PENDING") && !delivery.status().equals("ENQUEUED")
                && !delivery.status().equals("RETRY_SCHEDULED")) {
            throw new IllegalStateException("Delivery and work state disagree");
        }
        UUID token = Objects.requireNonNull(tokens.get());
        Instant expiry = now.plusSeconds(60);
        Instant deadline = now.plusSeconds(240);
        int changedWork = jdbc.update("""
                UPDATE delivery_work SET status = 'IN_PROGRESS', lease_owner = ?, lease_token = ?,
                    lease_expires_at = ?, processing_deadline_at = ?, claim_count = claim_count + 1,
                    updated_at = clock_timestamp()
                WHERE delivery_id = ? AND status IN ('PENDING', 'ENQUEUED', 'RETRY_SCHEDULED')
                """, workerId, token, Timestamp.from(expiry), Timestamp.from(deadline), deliveryId);
        int changedDelivery = jdbc.update("""
                UPDATE deliveries SET status = 'IN_PROGRESS', lease_owner = ?, lease_token = ?,
                    lease_expires_at = ?, processing_deadline_at = ?, claim_count = claim_count + 1,
                    updated_at = clock_timestamp(), version = version + 1
                WHERE accepted_day = ? AND id = ? AND organization_id = ?
                    AND status IN ('PENDING', 'ENQUEUED', 'RETRY_SCHEDULED')
                    AND version = ?
                """, workerId, token, Timestamp.from(expiry), Timestamp.from(deadline), work.day(),
                deliveryId, work.organizationId(), delivery.version());
        requirePair(changedWork, changedDelivery);
        return Optional.of(new DeliveryClaimService.Lease(deliveryId, work.day(), work.organizationId(),
                work.endpointId(), workerId, token, delivery.version() + 1, now, expiry, deadline));
    }

    @Override
    public Optional<DeliveryClaimService.Lease> renew(DeliveryClaimService.Lease lease) {
        Instant now = databaseTime();
        List<Work> rows = jdbc.query("""
                SELECT accepted_day, organization_id, endpoint_id, lease_token
                FROM delivery_work WHERE delivery_id = ? AND status = 'IN_PROGRESS'
                    AND lease_owner = ? AND lease_token = ? AND lease_expires_at > ?
                    AND processing_deadline_at > ? FOR UPDATE
                """, (row, index) -> new Work(row.getObject(1, LocalDate.class),
                row.getObject(2, UUID.class), row.getObject(3, UUID.class),
                row.getObject(4, UUID.class)), lease.deliveryId(), lease.workerId(), lease.token(),
                Timestamp.from(now), Timestamp.from(now));
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        Work work = rows.getFirst();
        if (!work.day().equals(lease.acceptedDay()) || !work.organizationId().equals(lease.organizationId())
                || !work.endpointId().equals(lease.endpointId())) {
            return Optional.empty();
        }
        Delivery delivery = lockDelivery(work.day(), lease.deliveryId());
        if (!"IN_PROGRESS".equals(delivery.status()) || delivery.version() != lease.version()
                || !lease.token().equals(delivery.token())) {
            return Optional.empty();
        }
        Instant expiry = now.plusSeconds(60).isBefore(lease.deadline())
                ? now.plusSeconds(60) : lease.deadline();
        if (!expiry.isAfter(now)) {
            return Optional.empty();
        }
        int changedWork = jdbc.update("""
                UPDATE delivery_work SET lease_expires_at = ?, updated_at = clock_timestamp()
                WHERE delivery_id = ? AND lease_token = ? AND processing_deadline_at = ?
                """, Timestamp.from(expiry), lease.deliveryId(), lease.token(),
                Timestamp.from(lease.deadline()));
        int changedDelivery = jdbc.update("""
                UPDATE deliveries SET lease_expires_at = ?, updated_at = clock_timestamp(),
                    version = version + 1
                WHERE accepted_day = ? AND id = ? AND organization_id = ?
                    AND lease_token = ? AND version = ? AND processing_deadline_at = ?
                """, Timestamp.from(expiry), lease.acceptedDay(), lease.deliveryId(),
                lease.organizationId(), lease.token(), lease.version(), Timestamp.from(lease.deadline()));
        requirePair(changedWork, changedDelivery);
        return Optional.of(new DeliveryClaimService.Lease(lease.deliveryId(), lease.acceptedDay(),
                lease.organizationId(), lease.endpointId(), lease.workerId(), lease.token(),
                lease.version() + 1, lease.claimedAt(), expiry, lease.deadline()));
    }

    @Override
    public boolean owns(DeliveryClaimService.Lease lease) {
        Boolean owned = jdbc.queryForObject("""
                SELECT EXISTS (
                    SELECT 1 FROM delivery_work work JOIN deliveries delivery
                        ON delivery.accepted_day = work.accepted_day AND delivery.id = work.delivery_id
                        AND delivery.organization_id = work.organization_id
                    WHERE work.delivery_id = ? AND work.status = 'IN_PROGRESS'
                        AND delivery.status = 'IN_PROGRESS' AND work.lease_token = ?
                        AND delivery.lease_token = ? AND work.lease_owner = ?
                        AND delivery.lease_owner = ? AND delivery.version = ?
                        AND work.lease_expires_at > clock_timestamp()
                        AND delivery.lease_expires_at > clock_timestamp()
                        AND work.processing_deadline_at > clock_timestamp()
                )
                """, Boolean.class, lease.deliveryId(), lease.token(), lease.token(),
                lease.workerId(), lease.workerId(), lease.version());
        return Boolean.TRUE.equals(owned);
    }

    @Override
    public int recoverExpired(int limit) {
        Instant now = databaseTime();
        List<Recovery> expired = jdbc.query("""
                SELECT delivery_id, accepted_day, organization_id, lease_token, event_expires_at
                FROM delivery_work WHERE status = 'IN_PROGRESS' AND lease_expires_at <= ?
                ORDER BY lease_expires_at, delivery_id LIMIT ? FOR UPDATE SKIP LOCKED
                """, (row, index) -> new Recovery(row.getObject(1, UUID.class),
                row.getObject(2, LocalDate.class), row.getObject(3, UUID.class),
                row.getObject(4, UUID.class), row.getTimestamp(5).toInstant()),
                Timestamp.from(now), limit);
        for (Recovery work : expired) {
            Delivery delivery = lockDelivery(work.day(), work.deliveryId());
            if (!"IN_PROGRESS".equals(delivery.status()) || !work.token().equals(delivery.token())) {
                throw new IllegalStateException("Delivery and work lease disagree");
            }
            if (!work.eventExpiresAt().isAfter(now)) {
                int changedDelivery = jdbc.update("""
                        UPDATE deliveries SET status = 'DEAD_LETTERED',
                            last_error_code = 'EVENT_EXPIRED', completed_at = clock_timestamp(),
                            lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
                            processing_deadline_at = NULL, updated_at = clock_timestamp(),
                            version = version + 1
                        WHERE accepted_day = ? AND id = ? AND organization_id = ?
                            AND lease_token = ? AND status = 'IN_PROGRESS'
                        """, work.day(), work.deliveryId(), work.organizationId(), work.token());
                int changedWork = jdbc.update("""
                        DELETE FROM delivery_work WHERE delivery_id = ? AND lease_token = ?
                            AND status = 'IN_PROGRESS'
                        """, work.deliveryId(), work.token());
                requirePair(changedWork, changedDelivery);
                markAttemptUnknown(work);
                continue;
            }
            String next = delivery.attemptCount() == 0 ? "PENDING" : "RETRY_SCHEDULED";
            int changedWork = jdbc.update("""
                    UPDATE delivery_work SET status = ?, lease_owner = NULL, lease_token = NULL,
                        lease_expires_at = NULL, processing_deadline_at = NULL,
                        next_attempt_at = ?, updated_at = clock_timestamp()
                    WHERE delivery_id = ? AND lease_token = ? AND status = 'IN_PROGRESS'
                    """, next, Timestamp.from(now), work.deliveryId(), work.token());
            int changedDelivery = jdbc.update("""
                    UPDATE deliveries SET status = ?, lease_owner = NULL, lease_token = NULL,
                        lease_expires_at = NULL, processing_deadline_at = NULL,
                        next_attempt_at = ?, updated_at = clock_timestamp(), version = version + 1
                    WHERE accepted_day = ? AND id = ? AND organization_id = ?
                        AND lease_token = ? AND status = 'IN_PROGRESS'
                    """, next, Timestamp.from(now), work.day(), work.deliveryId(),
                    work.organizationId(), work.token());
            requirePair(changedWork, changedDelivery);
            markAttemptUnknown(work);
            if (delivery.attemptCount() == 0) {
                jdbc.update("""
                        INSERT INTO delivery_outbox (organization_id, accepted_day, event_id,
                            delivery_id, message_type, topic, partition_key, payload)
                        VALUES (?, ?, ?, ?, 'delivery.ready.v1', 'relayforge.delivery.ready.v1', ?,
                            jsonb_build_object('delivery_id', ?::text))
                        """, work.organizationId(), work.day(), delivery.eventId(),
                        work.deliveryId(), work.deliveryId().toString(), work.deliveryId().toString());
            }
        }
        return expired.size();
    }

    private void markAttemptUnknown(Recovery work) {
        jdbc.update("""
                UPDATE delivery_attempts SET outcome = 'UNKNOWN', finished_at = clock_timestamp(),
                    error_code = 'LEASE_EXPIRED'
                WHERE accepted_day = ? AND delivery_id = ? AND organization_id = ?
                    AND outcome = 'IN_PROGRESS'
                """, work.day(), work.deliveryId(), work.organizationId());
    }

    private int activeCount(UUID endpointId, UUID organizationId, Instant now) {
        Integer count = jdbc.queryForObject("""
                SELECT count(*) FROM delivery_work WHERE status = 'IN_PROGRESS'
                    AND lease_expires_at > ? AND (?::uuid IS NULL OR endpoint_id = ?::uuid)
                    AND (?::uuid IS NULL OR organization_id = ?::uuid)
                """, Integer.class, Timestamp.from(now), endpointId, endpointId,
                organizationId, organizationId);
        return Objects.requireNonNull(count);
    }

    private Delivery lockDelivery(LocalDate day, UUID deliveryId) {
        return jdbc.query("""
                SELECT status, version, lease_token, attempt_count, event_id FROM deliveries
                WHERE accepted_day = ? AND id = ? FOR UPDATE
                """, (row, index) -> new Delivery(row.getString(1), row.getLong(2),
                row.getObject(3, UUID.class), row.getInt(4), row.getObject(5, UUID.class)), day, deliveryId)
                .stream().findFirst().orElseThrow(() -> new IllegalStateException("Delivery missing"));
    }

    private Instant databaseTime() {
        return Objects.requireNonNull(jdbc.queryForObject("SELECT clock_timestamp()",
                Timestamp.class)).toInstant();
    }

    private void requirePair(int changedWork, int changedDelivery) {
        if (changedWork != 1 || changedDelivery != 1) {
            throw new IllegalStateException("Delivery lease transition did not update both rows");
        }
    }

    private record Work(LocalDate day, UUID organizationId, UUID endpointId, UUID token) {
    }

    private record Delivery(String status, long version, UUID token, int attemptCount, UUID eventId) {
    }

    private record Recovery(UUID deliveryId, LocalDate day, UUID organizationId, UUID token,
            Instant eventExpiresAt) {
    }
}

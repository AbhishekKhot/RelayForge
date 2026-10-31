package com.relayforge.outbox;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;

public final class JdbcOutboxRepository implements OutboxRepository {
    private static final long MAX_LEASE_MILLIS = Duration.ofMinutes(5).toMillis();
    private static final long MAX_RETRY_MILLIS = Duration.ofHours(1).toMillis();
    private final JdbcTemplate jdbc;
    private final Owner owner;

    public JdbcOutboxRepository(JdbcTemplate jdbc, Owner owner) {
        this.jdbc = Objects.requireNonNull(jdbc);
        this.owner = Objects.requireNonNull(owner);
    }

    @Override
    public List<ClaimedOutboxRow> claim(String workerId, int batchSize, Duration lease) {
        if (workerId == null || !workerId.matches("[A-Za-z0-9][A-Za-z0-9._:-]{0,159}")
                || batchSize < 1 || batchSize > 100) {
            throw new IllegalArgumentException("Invalid outbox claim request");
        }
        long leaseMillis = boundedMillis(lease, MAX_LEASE_MILLIS);
        String sql = """
                WITH batch AS (
                  SELECT id FROM %s
                  WHERE available_at <= clock_timestamp()
                    AND (status = 'PENDING'
                      OR (status = 'IN_FLIGHT' AND claim_expires_at < clock_timestamp()))
                  ORDER BY created_at, id
                  LIMIT ?
                  FOR UPDATE SKIP LOCKED
                )
                UPDATE %s o SET status = 'IN_FLIGHT', claimed_by = ?,
                  claim_token = gen_random_uuid(),
                  claim_expires_at = clock_timestamp() + (interval '1 millisecond' * ?::double precision),
                  publish_attempts = publish_attempts + 1
                FROM batch WHERE o.id = batch.id
                RETURNING o.id, o.organization_id, %s AS aggregate_id,
                  o.message_type, o.topic, o.partition_key,
                  CASE WHEN octet_length(o.payload::text) <= 65536
                    THEN o.payload::text END AS payload_json,
                  o.claim_token, o.publish_attempts, o.created_at
                """.formatted(owner.tableName, owner.tableName, owner.aggregateIdSql);
        return jdbc.query(sql, (row, index) -> new ClaimedOutboxRow(
                row.getObject("id", UUID.class), row.getObject("organization_id", UUID.class),
                row.getString("aggregate_id"), row.getString("message_type"),
                row.getString("topic"), row.getString("partition_key"),
                row.getString("payload_json"), row.getObject("claim_token", UUID.class),
                row.getInt("publish_attempts"), row.getTimestamp("created_at").toInstant()),
                batchSize, workerId, leaseMillis);
    }

    @Override
    public boolean markPublished(UUID id, UUID claimToken) {
        requireIdentity(id, claimToken);
        return jdbc.update("""
                UPDATE %s SET status = 'PUBLISHED', published_at = clock_timestamp(),
                  claimed_by = NULL, claim_token = NULL, claim_expires_at = NULL,
                  last_error_code = NULL
                WHERE id = ? AND status = 'IN_FLIGHT' AND claim_token = ?
                """.formatted(owner.tableName), id, claimToken) == 1;
    }

    @Override
    public boolean retry(UUID id, UUID claimToken, Duration delay, String errorCode) {
        requireIdentity(id, claimToken);
        long delayMillis = boundedMillis(delay, MAX_RETRY_MILLIS);
        if (errorCode == null || !errorCode.matches("[A-Z][A-Z0-9_]{0,79}")) {
            throw new IllegalArgumentException("Invalid outbox error code");
        }
        return jdbc.update("""
                UPDATE %s SET status = 'PENDING', available_at = clock_timestamp()
                  + (interval '1 millisecond' * ?::double precision),
                  claimed_by = NULL, claim_token = NULL,
                  claim_expires_at = NULL, last_error_code = ?
                WHERE id = ? AND status = 'IN_FLIGHT' AND claim_token = ?
                """.formatted(owner.tableName), delayMillis, errorCode, id, claimToken) == 1;
    }

    private static long boundedMillis(Duration duration, long maximum) {
        if (duration == null) {
            throw new IllegalArgumentException("Invalid outbox duration");
        }
        long millis;
        try {
            millis = duration.toMillis();
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("Invalid outbox duration", overflow);
        }
        if (millis < 1 || millis > maximum) {
            throw new IllegalArgumentException("Invalid outbox duration");
        }
        return millis;
    }

    private static void requireIdentity(UUID id, UUID claimToken) {
        if (id == null || claimToken == null) {
            throw new IllegalArgumentException("Invalid outbox claim identity");
        }
    }

    public enum Owner {
        CONTROL("control_outbox", "o.aggregate_id"),
        INGESTION("ingestion_outbox", "o.partition_key"),
        DELIVERY("delivery_outbox", "COALESCE(o.delivery_id::text, o.event_id::text)");

        private final String tableName;
        private final String aggregateIdSql;

        Owner(String tableName, String aggregateIdSql) {
            this.tableName = tableName;
            this.aggregateIdSql = aggregateIdSql;
        }
    }
}

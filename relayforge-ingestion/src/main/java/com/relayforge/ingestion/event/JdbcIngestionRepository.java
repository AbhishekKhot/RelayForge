package com.relayforge.ingestion.event;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;

public final class JdbcIngestionRepository implements IngestionRepository {
    private final JdbcTemplate jdbc;

    public JdbcIngestionRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public boolean activeFence(int bucketId, long epoch) {
        return !jdbc.query("SELECT epoch FROM bucket_fence WHERE bucket_id = ? AND epoch = ?"
                        + " AND state = 'ACTIVE' FOR SHARE", (row, index) -> row.getLong(1),
                bucketId, epoch).isEmpty();
    }

    @Override
    public Optional<RegistryEntry> lockRegistry(UUID organizationId, String key) {
        return jdbc.query("""
                SELECT request_hash, event_locator, event_id, accepted_at, expires_at
                FROM idempotency_registry
                WHERE organization_id = ? AND idempotency_key = ? FOR UPDATE
                """, (row, index) -> new RegistryEntry(row.getBytes(1), row.getString(2),
                row.getObject(3, UUID.class), row.getTimestamp(4).toInstant(),
                row.getTimestamp(5).toInstant()), organizationId, key).stream().findFirst();
    }

    @Override
    public Instant databaseTime() {
        return jdbc.queryForObject("SELECT clock_timestamp()",
                (row, index) -> row.getTimestamp(1).toInstant());
    }

    @Override
    public boolean insertRegistry(UUID organizationId, String key, int bucketId, byte[] requestHash,
            UUID eventId, String locator, LocalDate day, Instant acceptedAt) {
        return jdbc.update("""
                INSERT INTO idempotency_registry (organization_id, idempotency_key, bucket_id,
                    request_hash, request_hash_version, event_id, event_locator, accepted_day,
                    accepted_at, expires_at)
                VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
                ON CONFLICT (organization_id, idempotency_key) DO NOTHING
                """, organizationId, key, bucketId, requestHash, eventId, locator, day,
                Timestamp.from(acceptedAt), Timestamp.from(acceptedAt.plus(2160, ChronoUnit.HOURS))) == 1;
    }

    @Override
    public void replaceRegistry(UUID organizationId, String key, int bucketId, byte[] requestHash,
            UUID eventId, String locator, LocalDate day, Instant acceptedAt) {
        jdbc.update("""
                UPDATE idempotency_registry SET bucket_id = ?, request_hash = ?, request_hash_version = 1,
                    event_id = ?, event_locator = ?, accepted_day = ?, accepted_at = ?, expires_at = ?
                WHERE organization_id = ? AND idempotency_key = ?
                """, bucketId, requestHash, eventId, locator, day, Timestamp.from(acceptedAt),
                Timestamp.from(acceptedAt.plus(2160, ChronoUnit.HOURS)), organizationId, key);
    }

    @Override
    public boolean reserveRate(UUID organizationId, Instant acceptedAt, int limit) {
        Instant minute = acceptedAt.truncatedTo(ChronoUnit.MINUTES);
        return jdbc.update("""
                INSERT INTO ingestion_rate_counters (organization_id, minute_start, accepted_count)
                VALUES (?, ?, 1)
                ON CONFLICT (organization_id, minute_start) DO UPDATE
                    SET accepted_count = ingestion_rate_counters.accepted_count + 1
                WHERE ingestion_rate_counters.accepted_count < ?
                """, organizationId, Timestamp.from(minute), limit) == 1;
    }

    @Override
    public int deleteExpiredRateCounters(int maximumRows) {
        if (maximumRows < 1 || maximumRows > 1000) {
            throw new IllegalArgumentException("Invalid cleanup batch size");
        }
        return jdbc.update("""
                DELETE FROM ingestion_rate_counters
                WHERE ctid IN (
                    SELECT ctid FROM ingestion_rate_counters
                    WHERE minute_start < clock_timestamp() - interval '2 hours'
                    ORDER BY minute_start LIMIT ? FOR UPDATE SKIP LOCKED
                )
                """, maximumRows);
    }

    @Override
    public void insertEvent(UUID organizationId, String key, UUID eventId, LocalDate day,
            Instant acceptedAt, EventPayload payload) {
        jdbc.update("""
                INSERT INTO events (accepted_day, id, organization_id, event_type, occurred_at,
                    schema_version, payload, payload_hash, idempotency_key, accepted_at)
                VALUES (?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?)
                """, day, eventId, organizationId, payload.eventType(),
                payload.occurredAt() == null ? null : Timestamp.from(payload.occurredAt()),
                payload.schemaVersion(), payload.payloadJson(), payload.payloadHash(), key,
                Timestamp.from(acceptedAt));
    }

    @Override
    public void insertOutbox(UUID organizationId, UUID eventId, LocalDate day, String locator,
            String messageJson) {
        jdbc.update("""
                INSERT INTO ingestion_outbox (organization_id, accepted_day, event_id, message_type,
                    topic, partition_key, payload)
                VALUES (?, ?, ?, 'event.accepted.v1', 'relayforge.event.accepted.v1', ?, ?::jsonb)
                """, organizationId, day, eventId, locator, messageJson);
    }

    @Override
    public Optional<StoredEvent> findEvent(UUID organizationId, LocalDate day, UUID eventId) {
        return jdbc.query("""
                SELECT event_type, schema_version,
                    CASE WHEN octet_length(payload::text) <= 524288 THEN payload::text END,
                    occurred_at, accepted_at
                FROM events WHERE organization_id = ? AND accepted_day = ? AND id = ?
                """, (row, index) -> new StoredEvent(row.getString(1), row.getInt(2),
                row.getString(3), row.getTimestamp(4) == null ? null : row.getTimestamp(4).toInstant(),
                row.getTimestamp(5).toInstant()), organizationId, day, eventId).stream().findFirst();
    }
}

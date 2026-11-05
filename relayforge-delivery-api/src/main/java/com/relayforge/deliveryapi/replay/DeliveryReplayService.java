package com.relayforge.deliveryapi.replay;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

public final class DeliveryReplayService {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final Clock clock;

    public DeliveryReplayService(JdbcTemplate jdbc, TransactionTemplate transactions, Clock clock) {
        this.jdbc = Objects.requireNonNull(jdbc);
        this.transactions = Objects.requireNonNull(transactions);
        this.clock = Objects.requireNonNull(clock);
    }

    ReplayResult replay(UUID organization, UUID actorKeyId, UUID sourceId, String key,
            String reason, String requestId) {
        return replay(organization, actorKeyId, sourceId, key, reason, requestId, 100);
    }

    public ReplayResult replay(UUID organization, UUID actorKeyId, UUID sourceId, String key,
            String reason, String requestId, int maximumGeneration) {
        if (organization == null || actorKeyId == null || sourceId == null || key == null || key.isBlank()
                || key.length() > 255 || !key.equals(key.trim()) || key.chars().anyMatch(Character::isISOControl)
                || reason != null && (reason.isBlank() || reason.length() > 500)
                || maximumGeneration < 1 || maximumGeneration > 100) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        }
        byte[] hash = hash(reason);
        try {
            return transactions.execute(status -> create(organization, actorKeyId,
                    sourceId, key, hash, reason, requestId, maximumGeneration));
        } catch (DataAccessException unavailable) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
        }
    }

    private ReplayResult create(UUID organization, UUID actorKeyId, UUID sourceId, String key, byte[] hash,
            String reason, String requestId, int maximumGeneration) {
        List<Source> sources = jdbc.query("""
                SELECT delivery.accepted_day, delivery.event_id, delivery.endpoint_id,
                    delivery.subscription_id, delivery.status, progress.event_accepted_at
                FROM deliveries delivery JOIN routing_progress progress
                    ON progress.accepted_day = delivery.accepted_day
                    AND progress.event_id = delivery.event_id
                    AND progress.organization_id = delivery.organization_id
                WHERE delivery.organization_id = ? AND delivery.id = ?
                ORDER BY delivery.accepted_day DESC LIMIT 1
                """, (row, index) -> new Source(row.getObject(1, java.time.LocalDate.class),
                row.getObject(2, UUID.class), row.getObject(3, UUID.class),
                row.getObject(4, UUID.class), row.getString(5), row.getTimestamp(6).toInstant()),
                organization, sourceId);
        if (sources.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        }
        Source source = sources.getFirst();
        jdbc.queryForObject("""
                SELECT event_id FROM routing_progress
                WHERE accepted_day = ? AND event_id = ? AND organization_id = ? FOR UPDATE
                """, UUID.class, source.day(), source.event(), organization);
        Instant now = clock.instant();
        if (!source.acceptedAt().plusSeconds(2160L * 3600).isAfter(now)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT);
        }
        List<Existing> existing = jdbc.query("""
                SELECT request_hash, replay_delivery_id, delivery.generation
                FROM replay_requests request JOIN deliveries delivery
                    ON delivery.accepted_day = request.accepted_day
                    AND delivery.id = request.replay_delivery_id
                    AND delivery.organization_id = request.organization_id
                WHERE request.accepted_day = ? AND request.organization_id = ?
                    AND request.original_delivery_id = ? AND request.replay_key = ?
                """, (row, index) -> new Existing(row.getBytes(1),
                row.getObject(2, UUID.class), row.getInt(3)),
                source.day(), organization, sourceId, key);
        if (!existing.isEmpty()) {
            Existing prior = existing.getFirst();
            if (!MessageDigest.isEqual(hash, prior.hash())) {
                throw new ResponseStatusException(HttpStatus.CONFLICT);
            }
            return new ReplayResult(sourceId, prior.delivery(), prior.generation(), "PENDING");
        }
        if (!"DEAD_LETTERED".equals(source.status())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT);
        }
        List<String> endpoints = jdbc.query("""
                SELECT status FROM endpoint_projection WHERE organization_id = ? AND endpoint_id = ?
                FOR SHARE
                """, (row, index) -> row.getString(1), organization, source.endpoint());
        if (endpoints.size() != 1 || !"ACTIVE".equals(endpoints.getFirst())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT);
        }
        int generation = jdbc.queryForObject("""
                SELECT coalesce(max(generation), 0) + 1 FROM deliveries
                WHERE accepted_day = ? AND organization_id = ? AND event_id = ? AND endpoint_id = ?
                """, Integer.class, source.day(), organization, source.event(), source.endpoint());
        if (generation > maximumGeneration) {
            throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS);
        }
        UUID replayId = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO deliveries (accepted_day, id, organization_id, event_id, endpoint_id,
                    subscription_id, generation, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING')
                """, source.day(), replayId, organization, source.event(), source.endpoint(),
                source.subscription(), generation);
        jdbc.update("""
                INSERT INTO replay_requests (accepted_day, organization_id, original_delivery_id,
                    replay_delivery_id, replay_key, request_hash, requested_by_key_id, reason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, source.day(), organization, sourceId, replayId, key, hash, actorKeyId, reason);
        jdbc.update("""
                INSERT INTO delivery_audit_records (accepted_day, organization_id, delivery_id,
                    actor_key_id, action, request_id, details)
                VALUES (?, ?, ?, ?, 'delivery.replayed', ?, jsonb_build_object(
                    'original_delivery_id', ?::text, 'generation', ?::integer))
                """, source.day(), organization, replayId, actorKeyId, requestId,
                sourceId.toString(), generation);
        jdbc.update("""
                INSERT INTO delivery_work (delivery_id, accepted_day, organization_id, endpoint_id,
                    status, event_expires_at, next_attempt_at)
                VALUES (?, ?, ?, ?, 'PENDING', ?, ?)
                """, replayId, source.day(), organization, source.endpoint(),
                Timestamp.from(source.acceptedAt().plusSeconds(2160L * 3600)), Timestamp.from(now));
        jdbc.update("""
                INSERT INTO delivery_outbox (organization_id, accepted_day, event_id, delivery_id,
                    message_type, topic, partition_key, payload)
                VALUES (?, ?, ?, ?, 'delivery.ready.v1', 'relayforge.delivery.ready.v1', ?,
                    jsonb_build_object('delivery_id', ?::text))
                """, organization, source.day(), source.event(), replayId,
                replayId.toString(), replayId.toString());
        return new ReplayResult(sourceId, replayId, generation, "PENDING");
    }

    private byte[] hash(String reason) {
        try {
            byte[] value = reason == null ? new byte[] {0} :
                    ("1" + reason).getBytes(StandardCharsets.UTF_8);
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("SHA-256 unavailable", unavailable);
        }
    }

    private record Source(java.time.LocalDate day, UUID event, UUID endpoint,
            UUID subscription, String status, Instant acceptedAt) {
    }

    private record Existing(byte[] hash, UUID delivery, int generation) {
    }

    public record ReplayResult(@JsonProperty("original_delivery_id") UUID originalDeliveryId,
            @JsonProperty("replay_delivery_id") UUID replayDeliveryId,
            int generation, String status) {
    }
}

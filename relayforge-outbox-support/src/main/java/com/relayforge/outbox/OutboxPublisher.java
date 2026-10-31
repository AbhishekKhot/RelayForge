package com.relayforge.outbox;

import java.time.Duration;
import java.util.Objects;
import java.util.random.RandomGenerator;

public final class OutboxPublisher {
    private static final long MAX_RETRY_MILLIS = 60000;
    private final OutboxRepository repository;
    private final OutboxTransport transport;
    private final OutboxEnvelope envelope;
    private final String workerId;
    private final int batchSize;
    private final Duration lease;
    private final RandomGenerator random;

    public OutboxPublisher(OutboxRepository repository, OutboxTransport transport, OutboxEnvelope envelope,
            String workerId, int batchSize, Duration lease, RandomGenerator random) {
        this.repository = Objects.requireNonNull(repository);
        this.transport = Objects.requireNonNull(transport);
        this.envelope = Objects.requireNonNull(envelope);
        this.random = Objects.requireNonNull(random);
        if (workerId == null || workerId.isBlank() || workerId.length() > 160
                || batchSize < 1 || batchSize > 100 || lease == null
                || lease.compareTo(Duration.ofSeconds(10)) < 0
                || lease.compareTo(Duration.ofMinutes(5)) > 0) {
            throw new IllegalArgumentException("Invalid publisher configuration");
        }
        this.workerId = workerId;
        this.batchSize = batchSize;
        this.lease = lease;
    }

    public int publishBatch() {
        int published = 0;
        for (ClaimedOutboxRow row : repository.claim(workerId, batchSize, lease)) {
            String serialized;
            try {
                serialized = envelope.encode(row);
            } catch (IllegalArgumentException invalid) {
                retry(row, "INVALID_ENVELOPE");
                continue;
            }
            try {
                transport.publish(row.topic(), row.partitionKey(), serialized);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Outbox publication interrupted");
            } catch (Exception failed) {
                retry(row, "PUBLISH_FAILED");
                continue;
            }
            if (repository.markPublished(row.id(), row.claimToken())) {
                published++;
            }
        }
        return published;
    }

    private void retry(ClaimedOutboxRow row, String errorCode) {
        long cap = Math.min(MAX_RETRY_MILLIS,
                1000L << Math.min(6, Math.max(0, row.publishAttempts() - 1)));
        long delayMillis = Math.max(100, random.nextLong(cap + 1));
        repository.retry(row.id(), row.claimToken(), Duration.ofMillis(delayMillis), errorCode);
    }
}

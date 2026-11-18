package com.relayforge.outbox;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.random.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class OutboxPublisher {
    private static final Logger LOG = LoggerFactory.getLogger(OutboxPublisher.class);
    private static final long MAX_RETRY_MILLIS = 60000;
    private static final long MAX_POLL_NANOS = Duration.ofSeconds(5).toNanos();
    private final OutboxRepository repository;
    private final OutboxTransport transport;
    private final OutboxEnvelope envelope;
    private final String workerId;
    private final int maxRowsPerPoll;
    private final Duration lease;
    private final RandomGenerator random;
    private final LongSupplier monotonicNanos;
    private final MeterRegistry metrics;

    public OutboxPublisher(OutboxRepository repository, OutboxTransport transport, OutboxEnvelope envelope,
            String workerId, int maxRowsPerPoll, Duration lease, RandomGenerator random,
            MeterRegistry metrics) {
        this(repository, transport, envelope, workerId, maxRowsPerPoll, lease, random,
                System::nanoTime, metrics);
    }

    OutboxPublisher(OutboxRepository repository, OutboxTransport transport, OutboxEnvelope envelope,
            String workerId, int maxRowsPerPoll, Duration lease, RandomGenerator random,
            LongSupplier monotonicNanos, MeterRegistry metrics) {
        this.repository = Objects.requireNonNull(repository);
        this.transport = Objects.requireNonNull(transport);
        this.envelope = Objects.requireNonNull(envelope);
        this.random = Objects.requireNonNull(random);
        this.monotonicNanos = Objects.requireNonNull(monotonicNanos);
        this.metrics = Objects.requireNonNull(metrics);
        for (String stage : java.util.List.of("claim", "publish", "mark")) {
            metrics.timer("relayforge.outbox.stage.duration", "stage", stage);
        }
        if (workerId == null || workerId.isBlank() || workerId.length() > 160
                || maxRowsPerPoll < 1 || maxRowsPerPoll > 100 || lease == null
                || lease.compareTo(Duration.ofSeconds(10)) < 0
                || lease.compareTo(Duration.ofMinutes(5)) > 0) {
            throw new IllegalArgumentException("Invalid publisher configuration");
        }
        this.workerId = workerId;
        this.maxRowsPerPoll = maxRowsPerPoll;
        this.lease = lease;
    }

    public int publishBatch() {
        int published = 0;
        long started = monotonicNanos.getAsLong();
        for (int attempted = 0; attempted < maxRowsPerPoll; attempted++) {
            if (attempted > 0 && monotonicNanos.getAsLong() - started >= MAX_POLL_NANOS) {
                break;
            }
            var claimSample = Timer.start(metrics);
            java.util.List<ClaimedOutboxRow> rows;
            try {
                rows = repository.claim(workerId, 1, lease);
            } finally {
                claimSample.stop(metrics.timer("relayforge.outbox.stage.duration", "stage", "claim"));
            }
            if (rows.isEmpty()) {
                break;
            }
            ClaimedOutboxRow row = rows.getFirst();
            String serialized;
            try {
                serialized = envelope.encode(row);
            } catch (IllegalArgumentException invalid) {
                retry(row, "INVALID_ENVELOPE");
                continue;
            }
            var publishSample = Timer.start(metrics);
            try {
                transport.publish(row.topic(), row.partitionKey(), serialized);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Outbox publication interrupted");
            } catch (Exception failed) {
                retry(row, "PUBLISH_FAILED");
                continue;
            } finally {
                publishSample.stop(metrics.timer("relayforge.outbox.stage.duration",
                        "stage", "publish"));
            }
            var markSample = Timer.start(metrics);
            boolean marked;
            try {
                marked = repository.markPublished(row.id(), row.claimToken());
            } finally {
                markSample.stop(metrics.timer("relayforge.outbox.stage.duration", "stage", "mark"));
            }
            if (marked) {
                published++;
                LOG.info("Outbox message published organizationId={} eventId={} outboxId={} "
                                + "topic={} workerName={}", row.organizationId(), row.aggregateId(),
                        row.id(), row.topic(), workerId);
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

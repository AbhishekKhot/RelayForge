package com.relayforge.retryscheduler;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;

public final class RetrySchedulerMetrics {
    private final JdbcTemplate jdbc;
    private final AtomicLong dueBacklog = new AtomicLong();
    private final AtomicLong deliveryBacklog = new AtomicLong();
    private final AtomicLong oldestPendingAgeSeconds = new AtomicLong();
    private final AtomicLong oldestDueAgeSeconds = new AtomicLong();
    private final AtomicLong expiredLeases = new AtomicLong();
    private final AtomicLong exhaustedRows = new AtomicLong();
    private final AtomicLong outboxAgeSeconds = new AtomicLong();

    public RetrySchedulerMetrics(JdbcTemplate jdbc, MeterRegistry registry) {
        this.jdbc = Objects.requireNonNull(jdbc);
        Objects.requireNonNull(registry);
        Gauge.builder("relayforge.delivery.retry.due.backlog", dueBacklog, AtomicLong::get)
                .register(registry);
        Gauge.builder("relayforge.delivery.backlog", deliveryBacklog, AtomicLong::get)
                .register(registry);
        Gauge.builder("relayforge.delivery.oldest.pending.age.seconds", oldestPendingAgeSeconds,
                AtomicLong::get).register(registry);
        Gauge.builder("relayforge.delivery.retry.oldest.due.age.seconds", oldestDueAgeSeconds,
                AtomicLong::get).register(registry);
        Gauge.builder("relayforge.delivery.lease.expired", expiredLeases, AtomicLong::get)
                .register(registry);
        Gauge.builder("relayforge.delivery.retry.exhausted.rows", exhaustedRows, AtomicLong::get)
                .register(registry);
        Gauge.builder("relayforge.delivery.outbox.oldest.age.seconds", outboxAgeSeconds,
                AtomicLong::get).register(registry);
    }

    @Scheduled(fixedDelay = 10000)
    public void refresh() {
        deliveryBacklog.set(value("""
                SELECT count(*) FROM delivery_work
                WHERE status IN ('PENDING', 'ENQUEUED', 'RETRY_SCHEDULED')
                """));
        oldestPendingAgeSeconds.set(value("""
                SELECT COALESCE(GREATEST(0,
                    EXTRACT(EPOCH FROM (clock_timestamp() - min(delivery.created_at)))::bigint), 0)
                FROM delivery_work work JOIN deliveries delivery
                  ON delivery.accepted_day = work.accepted_day AND delivery.id = work.delivery_id
                WHERE work.status IN ('PENDING', 'ENQUEUED', 'RETRY_SCHEDULED')
                """));
        dueBacklog.set(value("""
                SELECT count(*) FROM delivery_work
                WHERE status = 'RETRY_SCHEDULED' AND next_attempt_at <= clock_timestamp()
                """));
        oldestDueAgeSeconds.set(value("""
                SELECT COALESCE(GREATEST(0,
                    EXTRACT(EPOCH FROM (clock_timestamp() - min(next_attempt_at)))::bigint), 0)
                FROM delivery_work
                WHERE status = 'RETRY_SCHEDULED' AND next_attempt_at <= clock_timestamp()
                """));
        expiredLeases.set(value("""
                SELECT count(*) FROM delivery_work
                WHERE status = 'IN_PROGRESS' AND lease_expires_at <= clock_timestamp()
                """));
        exhaustedRows.set(value("""
                SELECT count(*) FROM deliveries
                WHERE status = 'DEAD_LETTERED' AND last_error_code = 'MAX_ATTEMPTS'
                """));
        outboxAgeSeconds.set(value("""
                SELECT COALESCE(GREATEST(0,
                    EXTRACT(EPOCH FROM (clock_timestamp() - min(created_at)))::bigint), 0)
                FROM delivery_outbox
                WHERE status IN ('PENDING', 'IN_FLIGHT')
                    AND available_at <= clock_timestamp()
                """));
    }

    private long value(String sql) {
        return Objects.requireNonNull(jdbc.queryForObject(sql, Long.class));
    }
}

package com.relayforge.outbox;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;

public final class OutboxMetrics {
    private final JdbcTemplate jdbc;
    private final MeterRegistry registry;
    private final String owner;
    private final String table;
    private final AtomicLong pending = new AtomicLong(-1);
    private final AtomicLong oldestAgeSeconds = new AtomicLong(-1);
    private final AtomicLong expiredClaims = new AtomicLong(-1);

    public OutboxMetrics(JdbcTemplate jdbc, JdbcOutboxRepository.Owner owner,
            MeterRegistry registry) {
        this.jdbc = jdbc;
        this.registry = registry;
        this.owner = owner.name().toLowerCase(java.util.Locale.ROOT);
        this.table = switch (owner) {
            case CONTROL -> "control_outbox";
            case INGESTION -> "ingestion_outbox";
            case DELIVERY -> "delivery_outbox";
        };
        Gauge.builder("relayforge.outbox.pending", pending, AtomicLong::get)
                .tag("owner", this.owner).register(registry);
        Gauge.builder("relayforge.outbox.oldest.age.seconds", oldestAgeSeconds, AtomicLong::get)
                .tag("owner", this.owner).register(registry);
        Gauge.builder("relayforge.outbox.expired.claims", expiredClaims, AtomicLong::get)
                .tag("owner", this.owner).register(registry);
    }

    @Scheduled(fixedDelay = 10000)
    public void refresh() {
        try {
            jdbc.query("""
                    SELECT count(*) FILTER (WHERE status IN ('PENDING', 'IN_FLIGHT')),
                      COALESCE(GREATEST(0, EXTRACT(EPOCH FROM
                        (clock_timestamp() - min(created_at) FILTER
                          (WHERE status IN ('PENDING', 'IN_FLIGHT'))))::bigint), 0),
                      count(*) FILTER (WHERE status = 'IN_FLIGHT'
                        AND claim_expires_at <= clock_timestamp())
                    FROM %s
                    """.formatted(table), row -> {
                        pending.set(row.getLong(1));
                        oldestAgeSeconds.set(row.getLong(2));
                        expiredClaims.set(row.getLong(3));
                    });
        } catch (DataAccessException unavailable) {
            pending.set(-1);
            oldestAgeSeconds.set(-1);
            expiredClaims.set(-1);
        }
    }

    public void published(int count) {
        registry.counter("relayforge.outbox.publish", "owner", owner, "result", "published")
                .increment(count);
    }

    public void failed() {
        registry.counter("relayforge.outbox.publish", "owner", owner, "result", "failed")
                .increment();
    }
}

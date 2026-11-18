package com.relayforge.deliveryworker;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.atomic.AtomicInteger;

final class DeliveryHandlerMetrics {
    private final AtomicInteger active = new AtomicInteger();
    private final Timer duration;

    DeliveryHandlerMetrics(MeterRegistry registry) {
        Gauge.builder("relayforge.delivery.handlers.active", active, AtomicInteger::get)
                .register(registry);
        duration = registry.timer("relayforge.delivery.handler.duration");
    }

    void record(Runnable action) {
        active.incrementAndGet();
        try {
            duration.record(action);
        } finally {
            active.decrementAndGet();
        }
    }
}

package com.relayforge.deliveryworker;

import com.relayforge.deliveryworker.retry.RetryPolicy;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Objects;

public final class MicrometerDeliveryOutcomeRecorder implements DeliveryOutcomeRecorder {
    private final MeterRegistry registry;

    public MicrometerDeliveryOutcomeRecorder(MeterRegistry registry) {
        this.registry = Objects.requireNonNull(registry);
        for (RetryPolicy.Outcome outcome : RetryPolicy.Outcome.values()) {
            registry.counter("relayforge.delivery.attempts", "outcome", outcome.name().toLowerCase(
                    java.util.Locale.ROOT));
        }
        registry.timer("relayforge.delivery.attempt.duration");
    }

    @Override
    public void record(RetryPolicy.Outcome outcome, Duration elapsed) {
        registry.counter("relayforge.delivery.attempts", "outcome", outcome.name().toLowerCase(
                java.util.Locale.ROOT)).increment();
        registry.timer("relayforge.delivery.attempt.duration").record(elapsed);
    }
}

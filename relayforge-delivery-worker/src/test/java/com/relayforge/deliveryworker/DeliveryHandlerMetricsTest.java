package com.relayforge.deliveryworker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

class DeliveryHandlerMetricsTest {
    @Test
    void tracksActiveWorkAndRestoresGaugeAfterFailure() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        try {
            DeliveryHandlerMetrics metrics = new DeliveryHandlerMetrics(registry);
            metrics.record(() -> assertEquals(1.0,
                    registry.get("relayforge.delivery.handlers.active").gauge().value()));
            assertThrows(IllegalStateException.class, () -> metrics.record(() -> {
                assertEquals(1.0,
                        registry.get("relayforge.delivery.handlers.active").gauge().value());
                throw new IllegalStateException("failed");
            }));
            assertEquals(0.0, registry.get("relayforge.delivery.handlers.active").gauge().value());
            assertEquals(2, registry.get("relayforge.delivery.handler.duration").timer().count());
        } finally {
            registry.close();
        }
    }
}

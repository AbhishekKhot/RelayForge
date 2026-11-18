package com.relayforge.deliveryworker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

class DeliveryWorkerCapacityTest {
    @Test
    void defaultsPreserveExistingCapacity() {
        DeliveryWorkerCapacity capacity = DeliveryWorkerCapacity.from(new MockEnvironment());
        assertEquals(8, capacity.workers());
        assertEquals(8, capacity.databaseConnections());
        assertEquals(32, capacity.maxInFlight());
    }

    @Test
    void configuredWorkersExpandTheBoundedConsumerWindow() {
        MockEnvironment environment = new MockEnvironment()
                .withProperty("relayforge.delivery.consumer-workers", "16")
                .withProperty("relayforge.delivery.database-pool-size", "12");
        DeliveryWorkerCapacity capacity = DeliveryWorkerCapacity.from(environment);
        assertEquals(16, capacity.workers());
        assertEquals(12, capacity.databaseConnections());
        assertEquals(32, capacity.maxInFlight());
    }

    @Test
    void rejectsUnboundedOrInvalidCapacityAtStartup() {
        for (String value : new String[] {"0", "65"}) {
            MockEnvironment environment = new MockEnvironment()
                    .withProperty("relayforge.delivery.consumer-workers", value);
            assertThrows(IllegalArgumentException.class,
                    () -> DeliveryWorkerCapacity.from(environment));
        }
        MockEnvironment environment = new MockEnvironment()
                .withProperty("relayforge.delivery.database-pool-size", "0");
        assertThrows(IllegalArgumentException.class,
                () -> DeliveryWorkerCapacity.from(environment));
    }
}

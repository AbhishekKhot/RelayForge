package com.relayforge.deliveryworker;

import org.springframework.core.env.Environment;

public record DeliveryWorkerCapacity(int workers, int databaseConnections) {
    public DeliveryWorkerCapacity {
        if (workers < 1 || workers > 64 || databaseConnections < 1 || databaseConnections > 64) {
            throw new IllegalArgumentException("Delivery worker capacity must be between 1 and 64");
        }
    }

    public static DeliveryWorkerCapacity from(Environment environment) {
        return new DeliveryWorkerCapacity(
                environment.getProperty("relayforge.delivery.consumer-workers", Integer.class, 8),
                environment.getProperty("relayforge.delivery.database-pool-size", Integer.class, 8));
    }

    public int maxInFlight() {
        return Math.max(32, workers * 2);
    }
}

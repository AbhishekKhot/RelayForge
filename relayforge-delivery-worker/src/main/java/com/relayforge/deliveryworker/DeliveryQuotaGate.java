package com.relayforge.deliveryworker;

import java.util.UUID;

@FunctionalInterface
public interface DeliveryQuotaGate {
    boolean available();

    default boolean reserve(UUID organizationId, String permitId, int limit) {
        return available();
    }

    default void renew(UUID organizationId, String permitId) {
    }

    default void release(UUID organizationId, String permitId) {
    }
}

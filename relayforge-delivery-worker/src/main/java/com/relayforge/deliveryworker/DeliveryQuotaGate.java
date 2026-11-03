package com.relayforge.deliveryworker;

@FunctionalInterface
public interface DeliveryQuotaGate {
    boolean available();
}

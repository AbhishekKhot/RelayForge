package com.relayforge.ingestion.event;

@FunctionalInterface
public interface DurabilityGate {
    void verify();
}

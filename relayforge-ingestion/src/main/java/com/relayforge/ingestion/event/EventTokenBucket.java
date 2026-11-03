package com.relayforge.ingestion.event;

import java.util.UUID;

@FunctionalInterface
public interface EventTokenBucket {
    boolean tryAcquire(UUID organizationId, int eventsPerMinute, int burstEvents);
}

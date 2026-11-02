package com.relayforge.router.routing;

public interface RoutingRepository {
    void initialize(VerifiedEvent event, int maxTargets);

    int materializeChunk(VerifiedEvent event, int chunkSize);
}

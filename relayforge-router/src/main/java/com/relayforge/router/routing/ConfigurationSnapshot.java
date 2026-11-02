package com.relayforge.router.routing;

import java.util.List;
import java.util.UUID;

public record ConfigurationSnapshot(long revision, List<Endpoint> endpoints,
        List<Subscription> subscriptions) {
    public ConfigurationSnapshot {
        if (revision < 0 || endpoints == null || subscriptions == null) {
            throw new IllegalArgumentException("Invalid configuration snapshot");
        }
        endpoints = List.copyOf(endpoints);
        subscriptions = List.copyOf(subscriptions);
    }

    public record Endpoint(UUID id, String status, long version) {
    }

    public record Subscription(UUID id, UUID endpointId, String eventType,
            String status, long version, String filterConfig) {
    }
}

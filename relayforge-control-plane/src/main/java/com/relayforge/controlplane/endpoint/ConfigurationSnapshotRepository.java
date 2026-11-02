package com.relayforge.controlplane.endpoint;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface ConfigurationSnapshotRepository {
    Optional<Long> revision(UUID organizationId);

    List<EndpointState> endpoints(UUID organizationId, UUID afterId, int limit);

    List<SubscriptionState> subscriptions(UUID organizationId, UUID afterId, int limit);

    record EndpointState(@JsonProperty("endpoint_id") UUID endpointId,
            String status, long version) {
    }

    record SubscriptionState(@JsonProperty("subscription_id") UUID subscriptionId,
            @JsonProperty("endpoint_id") UUID endpointId,
            @JsonProperty("event_type") String eventType, String status, long version,
            @JsonProperty("filter_config") String filterConfig) {
    }
}

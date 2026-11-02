package com.relayforge.router.routing;

import java.util.List;
import java.util.UUID;

public interface ProjectionRepository {
    boolean ready(UUID organizationId);

    void replace(UUID organizationId, ConfigurationSnapshot snapshot);

    ApplyResult apply(UUID organizationId, long revision, ConfigurationSnapshot.Endpoint endpoint,
            List<ConfigurationSnapshot.Subscription> subscriptions);

    enum ApplyResult {
        APPLIED, DUPLICATE, GAP
    }
}

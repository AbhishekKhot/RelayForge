package com.relayforge.router.routing;

import java.util.UUID;

@FunctionalInterface
public interface ConfigurationSnapshotClient {
    ConfigurationSnapshot load(UUID organizationId);
}

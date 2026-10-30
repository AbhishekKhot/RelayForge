package com.relayforge.ingestion.event;

import java.util.UUID;

@FunctionalInterface
public interface OrganizationAuthenticator {
    AuthenticatedOrganization authenticate(String authorization);

    record AuthenticatedOrganization(UUID id, int eventsPerMinute) {
    }
}

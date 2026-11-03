package com.relayforge.controlplane.organization;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public interface QuotaCache {
    Optional<Map<String, Integer>> find(UUID organizationId, long revision);

    void put(UUID organizationId, long revision, Map<String, Integer> quotas);
}

package com.relayforge.deliveryworker.auth;

import java.util.Objects;
import java.util.UUID;

public record TokenCacheKey(UUID organizationId, UUID endpointId,
        long authConfigVersion, long credentialVersion) {
    public TokenCacheKey {
        Objects.requireNonNull(organizationId);
        Objects.requireNonNull(endpointId);
        if (authConfigVersion < 1 || credentialVersion < 1) {
            throw new IllegalArgumentException("Invalid token cache version");
        }
    }

    public String redisKey() {
        return "relayforge:token:v1:" + organizationId + ':' + endpointId + ':'
                + authConfigVersion + ':' + credentialVersion;
    }
}

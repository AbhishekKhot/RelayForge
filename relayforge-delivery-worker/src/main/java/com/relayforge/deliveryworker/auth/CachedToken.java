package com.relayforge.deliveryworker.auth;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

public record CachedToken(String token, String tokenType, Instant expiresAt,
        Instant cacheUntil, long authConfigVersion, long credentialVersion) {
    public CachedToken {
        Objects.requireNonNull(token);
        Objects.requireNonNull(tokenType);
        Objects.requireNonNull(expiresAt);
        Objects.requireNonNull(cacheUntil);
        if (!tokenType.equals("Bearer") || !cacheUntil.isBefore(expiresAt)
                || authConfigVersion < 1 || credentialVersion < 1) {
            throw new IllegalArgumentException("Invalid cached token");
        }
    }

    public boolean usable(TokenCacheKey key, Instant now) {
        return authConfigVersion == key.authConfigVersion()
                && credentialVersion == key.credentialVersion()
                && cacheUntil.isAfter(now) && expiresAt.isAfter(now.plusSeconds(5));
    }

    public AuthMaterial material() {
        return new AuthMaterial(Map.of("Authorization", tokenType + ' ' + token));
    }

    @Override
    public String toString() {
        return "CachedToken[token=[REDACTED], tokenType=" + tokenType
                + ", expiresAt=" + expiresAt + ", cacheUntil=" + cacheUntil + "]";
    }
}

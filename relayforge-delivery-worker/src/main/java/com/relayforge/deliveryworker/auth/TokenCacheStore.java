package com.relayforge.deliveryworker.auth;

import java.time.Duration;
import java.util.Optional;

public interface TokenCacheStore {
    Optional<CachedToken> get(TokenCacheKey key);

    boolean acquireLock(TokenCacheKey key, String owner, Duration ttl);

    void put(TokenCacheKey key, CachedToken token, Duration ttl);

    void releaseLock(TokenCacheKey key, String owner);

    void invalidateIfToken(TokenCacheKey key, String token);
}

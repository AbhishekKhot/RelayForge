package com.relayforge.deliveryworker.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.relayforge.deliveryworker.lease.DeliveryClaimService;
import java.util.Objects;
import java.util.function.BooleanSupplier;

public final class TokenAuthenticationStrategy implements AuthenticationStrategy {
    private final TokenExchangeService tokens;
    private final TokenCacheService cache;

    public TokenAuthenticationStrategy(TokenExchangeService tokens) {
        this.tokens = Objects.requireNonNull(tokens);
        this.cache = null;
    }

    public TokenAuthenticationStrategy(TokenCacheService cache) {
        this.tokens = null;
        this.cache = Objects.requireNonNull(cache);
    }

    @Override
    public AuthMaterial resolve(JsonNode configuration, DeliveryClaimService.Lease lease,
            BooleanSupplier leaseValid) {
        if (cache != null) {
            throw new IllegalArgumentException("Token cache key is required");
        }
        return tokens.acquire(configuration, lease, leaseValid);
    }

    @Override
    public AuthMaterial resolve(JsonNode configuration, DeliveryClaimService.Lease lease,
            BooleanSupplier leaseValid, TokenCacheKey key) {
        return cache == null ? resolve(configuration, lease, leaseValid)
                : cache.resolve(configuration, lease, leaseValid, key);
    }

    @Override
    public AuthMaterial refresh(JsonNode configuration, DeliveryClaimService.Lease lease,
            BooleanSupplier leaseValid, TokenCacheKey key, AuthMaterial previous) {
        return cache == null ? resolve(configuration, lease, leaseValid)
                : cache.refresh(configuration, lease, leaseValid, key, previous);
    }
}

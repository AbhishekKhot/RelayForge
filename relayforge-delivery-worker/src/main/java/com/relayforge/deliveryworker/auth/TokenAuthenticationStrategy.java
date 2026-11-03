package com.relayforge.deliveryworker.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.relayforge.deliveryworker.lease.DeliveryClaimService;
import java.util.Objects;
import java.util.function.BooleanSupplier;

public final class TokenAuthenticationStrategy implements AuthenticationStrategy {
    private final TokenExchangeService tokens;

    public TokenAuthenticationStrategy(TokenExchangeService tokens) {
        this.tokens = Objects.requireNonNull(tokens);
    }

    @Override
    public AuthMaterial resolve(JsonNode configuration, DeliveryClaimService.Lease lease,
            BooleanSupplier leaseValid) {
        return tokens.acquire(configuration, lease, leaseValid);
    }
}

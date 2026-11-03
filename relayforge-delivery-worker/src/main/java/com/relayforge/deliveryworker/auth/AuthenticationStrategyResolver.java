package com.relayforge.deliveryworker.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.relayforge.deliveryworker.lease.DeliveryClaimService;
import java.util.Objects;
import java.util.function.BooleanSupplier;

public final class AuthenticationStrategyResolver {
    private final AuthenticationStrategy staticStrategy;
    private final AuthenticationStrategy tokenStrategy;

    public AuthenticationStrategyResolver(AuthenticationStrategy staticStrategy,
            AuthenticationStrategy tokenStrategy) {
        this.staticStrategy = Objects.requireNonNull(staticStrategy);
        this.tokenStrategy = Objects.requireNonNull(tokenStrategy);
    }

    public AuthMaterial resolve(JsonNode configuration, DeliveryClaimService.Lease lease,
            BooleanSupplier leaseValid) {
        if (configuration == null || !configuration.isObject()) {
            throw new IllegalArgumentException("Authentication configuration is unavailable");
        }
        return strategy(configuration).resolve(configuration, lease, leaseValid);
    }

    public boolean dynamic(JsonNode configuration) {
        return strategy(configuration) == tokenStrategy;
    }

    private AuthenticationStrategy strategy(JsonNode configuration) {
        if (configuration == null) {
            throw new IllegalArgumentException("Authentication configuration is unavailable");
        }
        return switch (configuration.path("type").asText("")) {
            case "NONE", "BEARER", "BASIC", "STATIC_HEADER", "STATIC_API_KEY" -> staticStrategy;
            case "CLIENT_CREDENTIALS", "TOKEN_EXCHANGE" -> tokenStrategy;
            default -> throw new IllegalArgumentException("Authentication strategy is unavailable");
        };
    }
}

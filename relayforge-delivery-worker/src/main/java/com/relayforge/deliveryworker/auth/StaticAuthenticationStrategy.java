package com.relayforge.deliveryworker.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.relayforge.deliveryworker.lease.DeliveryClaimService;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.function.BooleanSupplier;

public final class StaticAuthenticationStrategy implements AuthenticationStrategy {
    @Override
    public AuthMaterial resolve(JsonNode configuration, DeliveryClaimService.Lease lease,
            BooleanSupplier leaseValid) {
        if (configuration == null || !configuration.isObject()) {
            throw new IllegalArgumentException("Authentication configuration is unavailable");
        }
        return switch (configuration.path("type").asText("")) {
            case "NONE" -> new AuthMaterial(Map.of());
            case "BEARER" -> new AuthMaterial(Map.of("Authorization",
                    "Bearer " + required(configuration, "token")));
            case "BASIC" -> new AuthMaterial(Map.of("Authorization", "Basic "
                    + Base64.getEncoder().encodeToString((required(configuration, "username") + ":"
                    + required(configuration, "password")).getBytes(StandardCharsets.UTF_8))));
            case "STATIC_HEADER", "STATIC_API_KEY" -> {
                String name = required(configuration, "header_name");
                if (name.regionMatches(true, 0, "X-RelayForge-", 0, 13)
                        || name.equalsIgnoreCase("Authorization")) {
                    throw new IllegalArgumentException("Authentication header is invalid");
                }
                yield new AuthMaterial(Map.of(name, required(configuration, "value")));
            }
            default -> throw new IllegalArgumentException("Authentication strategy is unavailable");
        };
    }

    private String required(JsonNode object, String field) {
        JsonNode value = object.path(field);
        if (!value.isTextual() || value.asText().isBlank()) {
            throw new IllegalArgumentException("Authentication configuration is unavailable");
        }
        return value.asText();
    }
}

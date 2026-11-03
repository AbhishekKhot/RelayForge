package com.relayforge.deliveryworker.auth;

import java.util.Map;

public record AuthMaterial(Map<String, String> headers) {
    public AuthMaterial {
        headers = Map.copyOf(headers);
    }

    @Override
    public String toString() {
        return "AuthMaterial[headers=[REDACTED]]";
    }
}

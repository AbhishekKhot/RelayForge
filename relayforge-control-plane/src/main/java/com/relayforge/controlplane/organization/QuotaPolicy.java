package com.relayforge.controlplane.organization;

import java.util.HashMap;
import java.util.Map;

public final class QuotaPolicy {
    private static final Map<String, Integer> FREE = Map.of(
            "events_per_minute", 600,
            "burst_events", 20,
            "max_active_endpoints", 25,
            "max_in_flight_deliveries", 100,
            "max_event_payload_bytes", 262144,
            "max_replay_generations", 3);
    private static final Map<String, Integer> STANDARD = Map.of(
            "events_per_minute", 6000,
            "burst_events", 200,
            "max_active_endpoints", 100,
            "max_in_flight_deliveries", 1000,
            "max_event_payload_bytes", 262144,
            "max_replay_generations", 10);
    private static final Map<String, Integer> MAXIMUM = Map.of(
            "events_per_minute", 60000,
            "burst_events", 2000,
            "max_active_endpoints", 1000,
            "max_in_flight_deliveries", 20000,
            "max_event_payload_bytes", 262144,
            "max_replay_generations", 100);

    private QuotaPolicy() {
    }

    public static Map<String, Integer> resolve(String plan, Map<String, ?> overrides) {
        Map<String, Integer> defaults = switch (plan) {
            case "FREE" -> FREE;
            case "STANDARD" -> STANDARD;
            default -> throw new IllegalArgumentException("Unsupported plan");
        };
        Map<String, Integer> result = new HashMap<>(defaults);
        if (overrides != null) {
            for (Map.Entry<String, ?> entry : overrides.entrySet()) {
                Integer maximum = MAXIMUM.get(entry.getKey());
                if (maximum == null || !(entry.getValue() instanceof Integer value)
                        || value < 1 || value > maximum) {
                    throw new IllegalArgumentException("Invalid quota override");
                }
                result.put(entry.getKey(), value);
            }
        }
        if (result.get("burst_events") > result.get("events_per_minute")) {
            throw new IllegalArgumentException("Burst exceeds minute allowance");
        }
        return Map.copyOf(result);
    }
}

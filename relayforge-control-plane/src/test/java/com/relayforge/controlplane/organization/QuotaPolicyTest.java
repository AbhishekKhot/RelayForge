package com.relayforge.controlplane.organization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;

class QuotaPolicyTest {
    @Test
    void resolvesPlanDefaultsAndValidatedOverrides() {
        assertThat(QuotaPolicy.resolve("FREE", null)).containsEntry("events_per_minute", 600)
                .containsEntry("burst_events", 20)
                .containsEntry("max_active_endpoints", 25)
                .containsEntry("max_in_flight_deliveries", 100);
        assertThat(QuotaPolicy.resolve("STANDARD", Map.of("burst_events", 300)))
                .containsEntry("events_per_minute", 6000)
                .containsEntry("burst_events", 300)
                .containsEntry("max_in_flight_deliveries", 1000);
        assertThat(QuotaPolicy.resolve("FREE", Map.of("max_event_payload_bytes", 32768,
                "max_replay_generations", 7))).containsEntry("max_event_payload_bytes", 32768)
                .containsEntry("max_replay_generations", 7);
    }

    @Test
    void rejectsUnknownAndOutOfBoundsQuotaValues() {
        assertThatThrownBy(() -> QuotaPolicy.resolve("PREMIUM", null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> QuotaPolicy.resolve("FREE", Map.of("unlimited", 1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> QuotaPolicy.resolve("FREE", Map.of("burst_events", 2001)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> QuotaPolicy.resolve("FREE", Map.of("max_in_flight_deliveries", 20001)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> QuotaPolicy.resolve("FREE", Map.of("events_per_minute", 1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

package com.relayforge.analyticsapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;

class AnalyticsRangeTest {
    @Test
    void acceptsBoundedUtcRangeAndSupportedIntervals() {
        AnalyticsRange range = AnalyticsRange.parse("2026-11-01T00:00:00Z",
                "2026-11-02T00:00:00Z", "hour");
        assertThat(range.from()).isEqualTo(Instant.parse("2026-11-01T00:00:00Z"));
        assertThat(range.to()).isEqualTo(Instant.parse("2026-11-02T00:00:00Z"));
        assertThat(range.interval()).isEqualTo(AnalyticsRange.Interval.HOUR);
    }

    @Test
    void rejectsInvalidOrUnboundedRanges() {
        assertInvalid(null, "2026-11-02T00:00:00Z", "hour");
        assertInvalid("2026-11-02T00:00:00Z", "2026-11-01T00:00:00Z", "hour");
        assertInvalid("2026-01-01T00:00:00Z", "2026-03-01T00:00:00Z", "day");
        assertInvalid("2026-11-01T00:00:00+01:00", "2026-11-02T00:00:00Z", "hour");
        assertInvalid("2026-11-01T00:00:00.0001Z", "2026-11-02T00:00:00Z", "hour");
        assertInvalid("2026-11-01T00:00:00Z", "2026-11-02T00:00:00Z", null);
        assertInvalid("2026-11-01T00:00:00Z", "2026-11-02T00:00:00Z", "minute");
    }

    private void assertInvalid(String from, String to, String interval) {
        assertThatThrownBy(() -> AnalyticsRange.parse(from, to, interval))
                .isInstanceOf(ResponseStatusException.class)
                .extracting("statusCode.value").isEqualTo(400);
    }
}

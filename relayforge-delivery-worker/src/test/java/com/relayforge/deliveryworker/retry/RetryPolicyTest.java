package com.relayforge.deliveryworker.retry;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class RetryPolicyTest {
    private static final Instant NOW = Instant.parse("2026-11-02T10:00:00Z");
    private static final RetryPolicy.Settings SETTINGS = new RetryPolicy.Settings(
            Duration.ofSeconds(1), Duration.ofMinutes(15), Duration.ofHours(1),
            Duration.ofHours(24));

    @Test
    void classifiesResponsesAndNetworkFailures() {
        RetryPolicy policy = policy(0.5);
        assertThat(policy.decide(204, RetryPolicy.Failure.NONE, 1, 10,
                NOW.minusSeconds(10), NOW.plusSeconds(3600), null).outcome())
                .isEqualTo(RetryPolicy.Outcome.SUCCEEDED);
        for (int status : new int[]{408, 429, 500, 503, 599}) {
            assertThat(policy.decide(status, RetryPolicy.Failure.NONE, 1, 10,
                    NOW.minusSeconds(10), NOW.plusSeconds(3600), null).outcome())
                    .isEqualTo(RetryPolicy.Outcome.RETRY_SCHEDULED);
        }
        for (int status : new int[]{400, 401, 403, 404, 422}) {
            assertThat(policy.decide(status, RetryPolicy.Failure.NONE, 1, 10,
                    NOW.minusSeconds(10), NOW.plusSeconds(3600), null).outcome())
                    .isEqualTo(RetryPolicy.Outcome.DEAD_LETTERED);
        }
        assertThat(policy.decide(0, RetryPolicy.Failure.NETWORK, 1, 10,
                NOW.minusSeconds(10), NOW.plusSeconds(3600), null).outcome())
                .isEqualTo(RetryPolicy.Outcome.RETRY_SCHEDULED);
        assertThat(policy.decide(0, RetryPolicy.Failure.SECURITY, 1, 10,
                NOW.minusSeconds(10), NOW.plusSeconds(3600), null).outcome())
                .isEqualTo(RetryPolicy.Outcome.DEAD_LETTERED);
    }

    @Test
    void appliesFullJitterAndBoundedRetryAfter() {
        RetryPolicy policy = policy(0.5);
        assertThat(policy.decide(503, RetryPolicy.Failure.NONE, 3, 10,
                NOW.minusSeconds(10), NOW.plusSeconds(7200), "120").nextAttemptAt())
                .isEqualTo(NOW.plusSeconds(120));
        assertThat(policy.decide(429, RetryPolicy.Failure.NONE, 1, 10,
                NOW.minusSeconds(10), NOW.plusSeconds(7200), "99999").nextAttemptAt())
                .isEqualTo(NOW.plusSeconds(3600));
        assertThat(policy.decide(429, RetryPolicy.Failure.NONE, 1, 10,
                NOW.minusSeconds(10), NOW.plusSeconds(7200),
                "Mon, 02 Nov 2026 10:02:00 GMT").nextAttemptAt())
                .isEqualTo(NOW.plusSeconds(120));
        assertThat(policy.decide(500, RetryPolicy.Failure.NONE, 3, 10,
                NOW.minusSeconds(10), NOW.plusSeconds(7200), "120").nextAttemptAt())
                .isEqualTo(NOW.plusMillis(2000));
        assertThat(policy.decide(429, RetryPolicy.Failure.NONE, 1, 10,
                NOW.minusSeconds(10), NOW.plusSeconds(7200), "invalid").nextAttemptAt())
                .isEqualTo(NOW.plusMillis(500));
    }

    @Test
    void stopsAtAttemptLimitAndRetryHorizon() {
        RetryPolicy policy = policy(0.5);
        assertThat(policy.decide(500, RetryPolicy.Failure.NONE, 10, 10,
                NOW.minusSeconds(10), NOW.plusSeconds(7200), null).outcome())
                .isEqualTo(RetryPolicy.Outcome.DEAD_LETTERED);
        assertThat(policy.decide(500, RetryPolicy.Failure.NONE, 1, 10,
                NOW.minus(Duration.ofHours(24)), NOW.plusSeconds(7200), null).outcome())
                .isEqualTo(RetryPolicy.Outcome.DEAD_LETTERED);
        assertThat(policy.decide(503, RetryPolicy.Failure.NONE, 1, 10,
                NOW.minusSeconds(10), NOW.plusSeconds(30), "120").outcome())
                .isEqualTo(RetryPolicy.Outcome.DEAD_LETTERED);
    }

    private RetryPolicy policy(double draw) {
        return new RetryPolicy(java.time.Clock.fixed(NOW, java.time.ZoneOffset.UTC), () -> draw,
                SETTINGS);
    }
}

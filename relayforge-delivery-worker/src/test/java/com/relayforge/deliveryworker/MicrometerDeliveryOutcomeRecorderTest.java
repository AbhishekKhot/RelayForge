package com.relayforge.deliveryworker;

import com.relayforge.deliveryworker.retry.RetryPolicy;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MicrometerDeliveryOutcomeRecorderTest {
    @Test
    void recordsBoundedOutcomeAndAttemptDuration() {
        var registry = new SimpleMeterRegistry();
        var recorder = new MicrometerDeliveryOutcomeRecorder(registry);
        recorder.record(RetryPolicy.Outcome.RETRY_SCHEDULED, Duration.ofMillis(125));
        recorder.record(RetryPolicy.Outcome.SUCCEEDED, Duration.ofMillis(75));

        assertThat(registry.get("relayforge.delivery.attempts")
                .tag("outcome", "retry_scheduled").counter().count()).isEqualTo(1);
        assertThat(registry.get("relayforge.delivery.attempts")
                .tag("outcome", "succeeded").counter().count()).isEqualTo(1);
        assertThat(registry.get("relayforge.delivery.attempt.duration").timer().count())
                .isEqualTo(2);
    }
}

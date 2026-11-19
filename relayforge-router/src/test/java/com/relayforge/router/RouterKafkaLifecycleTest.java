package com.relayforge.router;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class RouterKafkaLifecycleTest {
    @Test
    void batchesConfigurationRecordsOnOneOrderedWorker() {
        var options = RouterKafkaLifecycle.configurationOptions(64);

        assertThat(options.workers()).isEqualTo(1);
        assertThat(options.maxInFlight()).isEqualTo(64);
    }

    @Test
    void rejectsUnboundedConfigurationIntake() {
        assertThatThrownBy(() -> RouterKafkaLifecycle.configurationOptions(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RouterKafkaLifecycle.configurationOptions(257))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

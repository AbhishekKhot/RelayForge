package com.relayforge.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

class KafkaConsumerSettingsTest {
    @Test
    void disablesAutoCommitAndBoundsPollSize() {
        var settings = KafkaConsumerSettings.properties("broker", "routing", 8, 8,
                Duration.ofSeconds(30), Duration.ofMillis(100), Duration.ofSeconds(2));
        assertThat(settings.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)).isEqualTo(false);
        assertThat(settings.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)).isEqualTo(8);
    }

    @Test
    void rejectsPollThatCanExceedCapacityOrInterval() {
        assertThrows(IllegalArgumentException.class, () -> KafkaConsumerSettings.properties(
                "broker", "routing", 9, 8, Duration.ofSeconds(30), Duration.ofMillis(100),
                Duration.ofSeconds(2)));
        assertThrows(IllegalArgumentException.class, () -> KafkaConsumerSettings.properties(
                "broker", "routing", 8, 8, Duration.ofSeconds(1), Duration.ofMillis(100),
                Duration.ofSeconds(2)));
    }
}

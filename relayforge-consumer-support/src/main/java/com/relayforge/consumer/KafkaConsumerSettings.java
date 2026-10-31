package com.relayforge.consumer;

import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

/** Creates settings for a manually acknowledged bounded consumer. */
public final class KafkaConsumerSettings {
    private KafkaConsumerSettings() { }

    public static Properties properties(String bootstrapServers, String groupId,
                                        int maxPollRecords, int maxInFlight,
                                        Duration maxPollInterval, Duration pollTimeout,
                                        Duration commitTimeout) {
        if (bootstrapServers == null || bootstrapServers.isBlank()
                || groupId == null || groupId.isBlank()
                || maxPollRecords < 1 || maxPollRecords > maxInFlight
                || maxPollInterval == null || pollTimeout == null || commitTimeout == null
                || maxPollInterval.toMillis() > Integer.MAX_VALUE
                || maxPollInterval.toMillis() <= pollTimeout.toMillis()
                        + 2 * commitTimeout.toMillis()) {
            throw new IllegalArgumentException("Invalid consumer settings");
        }
        Properties settings = new Properties();
        settings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        settings.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        settings.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        settings.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, (int) maxPollInterval.toMillis());
        return settings;
    }
}

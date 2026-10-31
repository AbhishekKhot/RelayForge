package com.relayforge.outbox;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public final class KafkaOutboxTransport implements OutboxTransport, AutoCloseable {
    private final Producer<String, String> producer;
    private final Duration acknowledgementTimeout;

    public KafkaOutboxTransport(String bootstrapServers, Duration acknowledgementTimeout) {
        this(newProducer(bootstrapServers, acknowledgementTimeout), acknowledgementTimeout);
    }

    KafkaOutboxTransport(Producer<String, String> producer, Duration acknowledgementTimeout) {
        this.producer = Objects.requireNonNull(producer);
        validateTimeout(acknowledgementTimeout);
        this.acknowledgementTimeout = acknowledgementTimeout;
    }

    private static Producer<String, String> newProducer(String bootstrapServers, Duration timeout) {
        validateTimeout(timeout);
        return new KafkaProducer<>(settings(bootstrapServers));
    }

    private static void validateTimeout(Duration timeout) {
        if (timeout == null || timeout.compareTo(Duration.ofSeconds(1)) < 0
                || timeout.compareTo(Duration.ofSeconds(20)) > 0) {
            throw new IllegalArgumentException("Invalid Kafka acknowledgement timeout");
        }
    }

    static Properties settings(String bootstrapServers) {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("Kafka bootstrap servers are required");
        }
        Properties settings = new Properties();
        settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        settings.put(ProducerConfig.ACKS_CONFIG, "all");
        settings.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        settings.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        settings.put(ProducerConfig.RETRIES_CONFIG, "3");
        settings.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        settings.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "3000");
        settings.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        settings.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "12000");
        return settings;
    }

    @Override
    public void publish(String topic, String partitionKey, String envelopeJson) throws Exception {
        var sent = producer.send(new ProducerRecord<>(topic, partitionKey, envelopeJson));
        try {
            sent.get(acknowledgementTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException timedOut) {
            sent.cancel(true);
            throw timedOut;
        }
    }

    @Override
    public void close() {
        producer.close(Duration.ofSeconds(5));
    }
}

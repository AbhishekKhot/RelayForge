package com.relayforge.router;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.apisupport.ReadinessProbe;
import com.relayforge.consumer.ConsumerOptions;
import com.relayforge.consumer.KafkaConsumerSettings;
import com.relayforge.consumer.MessageHandler;
import com.relayforge.consumer.ResilientKafkaConsumer;
import com.relayforge.contracts.EnvelopeValidator;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.SmartLifecycle;

public final class RouterKafkaLifecycle implements SmartLifecycle, ReadinessProbe {
    private static final String CONFIG_TOPIC = "relayforge.config.changed.v1";
    private static final String EVENT_TOPIC = "relayforge.event.accepted.v1";
    private final ResilientKafkaConsumer configConsumer;
    private final ResilientKafkaConsumer eventConsumer;
    private final AtomicBoolean running = new AtomicBoolean();
    private Thread configThread;
    private Thread eventThread;

    public RouterKafkaLifecycle(String bootstrapServers, String deliveryShardId, ObjectMapper json,
            MessageHandler configurationChanges, MessageHandler events) {
        if (deliveryShardId == null || !deliveryShardId.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,79}")) {
            throw new IllegalArgumentException("Invalid delivery shard");
        }
        EnvelopeValidator validator = new EnvelopeValidator(json);
        configConsumer = new ResilientKafkaConsumer(
                new KafkaConsumer<>(consumerSettings(bootstrapServers,
                        "relayforge-router-config-" + deliveryShardId, 1)),
                new KafkaProducer<>(producerSettings(bootstrapServers)), validator,
                configurationChanges, new ConsumerOptions(1, 1, 3, Duration.ofMillis(200),
                Duration.ofMillis(100), Duration.ofSeconds(2), Duration.ofSeconds(5)),
                List.of(CONFIG_TOPIC));
        eventConsumer = new ResilientKafkaConsumer(
                new KafkaConsumer<>(consumerSettings(bootstrapServers,
                        "relayforge-router-events-" + deliveryShardId, 32)),
                new KafkaProducer<>(producerSettings(bootstrapServers)), validator,
                events, new ConsumerOptions(4, 32, 3, Duration.ofMillis(200),
                Duration.ofMillis(100), Duration.ofSeconds(2), Duration.ofSeconds(5)),
                List.of(EVENT_TOPIC));
    }

    private Properties consumerSettings(String servers, String group, int maximumRecords) {
        Properties settings = KafkaConsumerSettings.properties(servers, group, maximumRecords,
                maximumRecords, Duration.ofSeconds(30), Duration.ofMillis(100), Duration.ofSeconds(2));
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return settings;
    }

    private Properties producerSettings(String servers) {
        Properties settings = new Properties();
        settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        settings.put(ProducerConfig.ACKS_CONFIG, "all");
        settings.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        settings.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        settings.put(ProducerConfig.RETRIES_CONFIG, 3);
        settings.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);
        settings.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        settings.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 12000);
        return settings;
    }

    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            configThread = Thread.ofPlatform().daemon(true).name("relayforge-router-config")
                    .start(configConsumer::run);
            eventThread = Thread.ofPlatform().daemon(true).name("relayforge-router-events")
                    .start(eventConsumer::run);
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            configConsumer.close();
            eventConsumer.close();
            join(configThread);
            join(eventThread);
        }
    }

    @Override
    public void stop(Runnable callback) {
        try {
            stop();
        } finally {
            callback.run();
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public boolean available() {
        return running.get() && configThread != null && configThread.isAlive()
                && eventThread != null && eventThread.isAlive();
    }

    private void join(Thread thread) {
        if (thread == null) {
            return;
        }
        try {
            thread.join(10000);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}

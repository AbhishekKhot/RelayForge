package com.relayforge.deliveryworker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.apisupport.ReadinessProbe;
import com.relayforge.consumer.ConsumerOptions;
import com.relayforge.consumer.KafkaConsumerSettings;
import com.relayforge.consumer.ResilientKafkaConsumer;
import com.relayforge.contracts.EnvelopeValidator;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.SmartLifecycle;

public final class DeliveryKafkaLifecycle implements SmartLifecycle, ReadinessProbe {
    private static final String TOPIC = "relayforge.delivery.ready.v1";
    private final ResilientKafkaConsumer consumer;
    private final AtomicBoolean running = new AtomicBoolean();
    private Thread thread;

    public DeliveryKafkaLifecycle(String bootstrapServers, ObjectMapper json,
            DeliveryProcessor processor, MeterRegistry registry) {
        Properties settings = KafkaConsumerSettings.properties(bootstrapServers,
                "relayforge-delivery-worker", 32, 32, Duration.ofMinutes(5),
                Duration.ofMillis(100), Duration.ofSeconds(2));
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        Properties producer = new Properties();
        producer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producer.put(ProducerConfig.ACKS_CONFIG, "all");
        producer.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        producer.put(ProducerConfig.RETRIES_CONFIG, 3);
        producer.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);
        producer.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        producer.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 12000);
        consumer = new ResilientKafkaConsumer(new KafkaConsumer<>(settings),
                new KafkaProducer<>(producer), new EnvelopeValidator(json), envelope -> {
                    if (!"delivery.ready.v1".equals(envelope.messageType())) {
                        throw new IllegalStateException("Unexpected delivery message");
                    }
                    String id = envelope.payload().path("delivery_id").asText("");
                    UUID deliveryId = UUID.fromString(id);
                    if (!deliveryId.toString().equals(envelope.aggregateId())) {
                        throw new IllegalStateException("Delivery identity mismatch");
                    }
                    processor.process(deliveryId);
                }, new ConsumerOptions(8, 32, 3, Duration.ofMillis(200),
                Duration.ofMillis(100), Duration.ofSeconds(2), Duration.ofSeconds(5)), List.of(TOPIC),
                registry, "delivery-worker");
    }

    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            thread = Thread.ofPlatform().daemon(true).name("relayforge-delivery-consumer")
                    .start(consumer::run);
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            consumer.close();
            if (thread != null) {
                try {
                    thread.join(10000);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
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
        return running.get() && thread != null && thread.isAlive();
    }
}

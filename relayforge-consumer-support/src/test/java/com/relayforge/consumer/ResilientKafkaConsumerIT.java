package com.relayforge.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.contracts.EnvelopeValidator;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;

class ResilientKafkaConsumerIT {
    private static final String TOPIC = "relayforge.delivery.ready.v1";
    private static final String QUARANTINE = TOPIC + ".quarantine";

    @Test
    void duplicateMessagesReachHandlerAndMalformedMessageIsQuarantined() throws Exception {
        try (KafkaContainer broker = new KafkaContainer("apache/kafka:4.1.2")) {
            broker.start();
            Properties producerSettings = producerSettings(broker.getBootstrapServers());
            try (Admin admin = Admin.create(producerSettings)) {
                admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1),
                        new NewTopic(QUARANTINE, 1, (short) 1))).all().get(20, TimeUnit.SECONDS);
            }
            try (KafkaProducer<String, String> input = new KafkaProducer<>(producerSettings)) {
                input.send(new ProducerRecord<>(TOPIC, "dlv_1", validEnvelope())).get();
                input.send(new ProducerRecord<>(TOPIC, "dlv_1", validEnvelope())).get();
                input.send(new ProducerRecord<>(TOPIC, "dlv_1", "private-secret-invalid-json")).get();
            }

            String group = "consumer-it-" + UUID.randomUUID();
            Properties consumerSettings = KafkaConsumerSettings.properties(
                    broker.getBootstrapServers(), group, 3, 3, Duration.ofSeconds(20),
                    Duration.ofMillis(50), Duration.ofSeconds(2));
            consumerSettings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            Set<String> logicalWrites = ConcurrentHashMap.newKeySet();
            AtomicInteger handlerCalls = new AtomicInteger();
            KafkaConsumer<String, String> source = new KafkaConsumer<>(consumerSettings);
            KafkaProducer<String, String> quarantineProducer = new KafkaProducer<>(producerSettings);
            ResilientKafkaConsumer loop = new ResilientKafkaConsumer(source, quarantineProducer,
                    new EnvelopeValidator(new ObjectMapper()),
                    envelope -> {
                        handlerCalls.incrementAndGet();
                        logicalWrites.add(envelope.aggregateId());
                    },
                    new ConsumerOptions(2, 3, 2, Duration.ofMillis(10), Duration.ofMillis(50),
                            Duration.ofSeconds(2), Duration.ofSeconds(5)), List.of(TOPIC));
            ExecutorService thread = Executors.newSingleThreadExecutor();
            var task = thread.submit(loop::run);
            try {
                assertThat(readQuarantine(broker.getBootstrapServers())).contains("MALFORMED_JSON")
                        .doesNotContain("private-secret-invalid-json");
                awaitCommitted(broker.getBootstrapServers(), group, 3);
                assertThat(handlerCalls).hasValue(2);
                assertThat(logicalWrites).containsExactly("dlv_1");
            } finally {
                loop.close();
                task.get(10, TimeUnit.SECONDS);
                thread.shutdownNow();
            }
        }
    }

    private String readQuarantine(String bootstrap) {
        Properties settings = KafkaConsumerSettings.properties(bootstrap, "quarantine-observer-" + UUID.randomUUID(),
                1, 1, Duration.ofSeconds(20), Duration.ofMillis(50), Duration.ofSeconds(2));
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<String, String> observer = new KafkaConsumer<>(settings)) {
            TopicPartition partition = new TopicPartition(QUARANTINE, 0);
            observer.assign(List.of(partition));
            observer.seekToBeginning(List.of(partition));
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (System.nanoTime() < end) {
                var records = observer.poll(Duration.ofMillis(100));
                if (!records.isEmpty()) {
                    return records.iterator().next().value();
                }
            }
            throw new AssertionError("Quarantine record missing");
        }
    }

    private void awaitCommitted(String bootstrap, String group, long expected) {
        Properties settings = KafkaConsumerSettings.properties(bootstrap, group, 1, 1,
                Duration.ofSeconds(20), Duration.ofMillis(50), Duration.ofSeconds(2));
        try (KafkaConsumer<String, String> inspector = new KafkaConsumer<>(settings)) {
            TopicPartition partition = new TopicPartition(TOPIC, 0);
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (System.nanoTime() < end) {
                var committed = inspector.committed(Set.of(partition));
                if (committed.get(partition) != null
                        && committed.get(partition).offset() >= expected) {
                    return;
                }
            }
            throw new AssertionError("Source offset was not committed");
        }
    }

    private Properties producerSettings(String bootstrap) {
        Properties settings = new Properties();
        settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        settings.put(ProducerConfig.ACKS_CONFIG, "all");
        settings.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return settings;
    }

    private String validEnvelope() {
        return "{\"message_id\":\"msg_8d78be41-a5d2-4faa-ae8d-bbc68c035f13\","
                + "\"message_type\":\"delivery.ready.v1\",\"schema_version\":1,"
                + "\"occurred_at\":\"2026-10-31T10:00:00Z\","
                + "\"organization_id\":\"de568400-3487-45de-a986-c02816a60011\","
                + "\"aggregate_id\":\"dlv_1\",\"correlation_id\":\"msg_x\","
                + "\"payload\":{\"delivery_id\":\"dlv_1\"}}";
    }
}

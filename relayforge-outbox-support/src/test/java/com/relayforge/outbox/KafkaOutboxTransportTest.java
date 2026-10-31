package com.relayforge.outbox;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KafkaOutboxTransportTest {
    @Test
    void configuresDurableBoundedProducer() {
        Properties settings = KafkaOutboxTransport.settings("kafka:9092");
        assertThat(settings.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo("kafka:9092");
        assertThat(settings.getProperty(ProducerConfig.ACKS_CONFIG)).isEqualTo("all");
        assertThat(settings.getProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)).isEqualTo("true");
        assertThat(Integer.parseInt(settings.getProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG)))
                .isLessThan(30000);
        assertThat(Integer.parseInt(settings.getProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)))
                .isLessThan(30000);
    }

    @Test
    void returnsOnlyAfterProducerAcknowledges() throws Exception {
        MockProducer<String, String> producer = new MockProducer<>(true,
                new StringSerializer(), new StringSerializer());
        try (KafkaOutboxTransport transport = new KafkaOutboxTransport(producer, Duration.ofSeconds(2))) {
            transport.publish("relayforge.event.accepted.v1", "evt_123", "{\"message_id\":\"msg_123\"}");
            assertThat(producer.history()).hasSize(1);
            assertThat(producer.history().getFirst().topic()).isEqualTo("relayforge.event.accepted.v1");
            assertThat(producer.history().getFirst().key()).isEqualTo("evt_123");
        }
    }

    @Test
    void waitsForAcknowledgementBeforeReturning() throws Exception {
        @SuppressWarnings("unchecked")
        Producer<String, String> producer = mock(Producer.class);
        CountDownLatch sent = new CountDownLatch(1);
        CompletableFuture<RecordMetadata> acknowledgement = new CompletableFuture<>();
        when(producer.send(any(ProducerRecord.class))).thenAnswer(invocation -> {
            sent.countDown();
            return acknowledgement;
        });
        var executor = Executors.newSingleThreadExecutor();
        try (KafkaOutboxTransport transport = new KafkaOutboxTransport(producer, Duration.ofSeconds(2))) {
            var completed = executor.submit(() -> {
                transport.publish("relayforge.event.accepted.v1", "evt_123", "{}");
                return null;
            });
            assertThat(sent.await(1, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> completed.get(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            acknowledgement.complete(null);
            completed.get(1, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void rejectsSubsecondAcknowledgementTimeout() {
        MockProducer<String, String> producer = new MockProducer<>(true,
                new StringSerializer(), new StringSerializer());
        assertThatThrownBy(() -> new KafkaOutboxTransport(producer, Duration.ofNanos(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

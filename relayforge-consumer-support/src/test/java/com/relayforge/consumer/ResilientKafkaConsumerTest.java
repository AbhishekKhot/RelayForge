package com.relayforge.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.contracts.EnvelopeValidator;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class ResilientKafkaConsumerTest {
    private static final String TOPIC = "relayforge.delivery.ready.v1";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
    private static final Duration WAIT = Duration.ofSeconds(2);

    @Test
    void waitsForEarlierWorkBeforeCommittingLaterOffset() throws Exception {
        Consumer<String, String> consumer = mockConsumer(records(validRecord(0), validRecord(1)));
        Producer<String, String> producer = mockProducer();
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch secondDone = new CountDownLatch(1);
        CountDownLatch firstDone = new CountDownLatch(1);
        MessageHandler handler = envelope -> {
            if (envelope.aggregateId().equals("dlv_0")) {
                releaseFirst.await();
                firstDone.countDown();
            } else {
                secondDone.countDown();
            }
        };
        ResilientKafkaConsumer loop = loop(consumer, producer, handler, 2);
        try {
            loop.runOnce();
            assertThat(secondDone.await(2, TimeUnit.SECONDS)).isTrue();
            loop.runOnce();
            verify(consumer, times(0)).commitSync(any(Map.class), any(Duration.class));
            releaseFirst.countDown();
            assertThat(firstDone.await(2, TimeUnit.SECONDS)).isTrue();
            loop.runOnce();
            ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> commits = ArgumentCaptor.forClass(Map.class);
            verify(consumer).commitSync(commits.capture(), any(Duration.class));
            assertThat(commits.getValue().get(PARTITION).offset()).isEqualTo(2);
        } finally {
            releaseFirst.countDown();
            loop.close();
        }
    }

    @Test
    void quarantineMustBeAcknowledgedBeforeSourceCommitAndContainsNoPayload() throws Exception {
        String secret = "secret-token-should-not-appear";
        Consumer<String, String> consumer = mockConsumer(records(
                new ConsumerRecord<>(TOPIC, 0, 5, "dlv_1", secret)));
        Producer<String, String> producer = mock(Producer.class);
        CompletableFuture<RecordMetadata> brokerAck = new CompletableFuture<>();
        CountDownLatch published = new CountDownLatch(1);
        when(producer.send(any())).thenAnswer(invocation -> {
            published.countDown();
            return brokerAck;
        });
        ResilientKafkaConsumer loop = loop(consumer, producer, ignored -> { }, 1);
        try {
            loop.runOnce();
            assertThat(published.await(2, TimeUnit.SECONDS)).isTrue();
            loop.runOnce();
            verify(consumer, times(0)).commitSync(any(Map.class), any(Duration.class));
            ArgumentCaptor<ProducerRecord<String, String>> sent = ArgumentCaptor.forClass(ProducerRecord.class);
            verify(producer).send(sent.capture());
            assertThat(sent.getValue().topic()).isEqualTo(TOPIC + ".quarantine");
            assertThat(sent.getValue().value()).doesNotContain(secret);
            assertThat(sent.getValue().value()).contains("MALFORMED_JSON", "source_offset");
            brokerAck.complete(null);
            awaitCommit(loop, consumer);
        } finally {
            brokerAck.complete(null);
            loop.close();
        }
    }

    @Test
    void retriesTransientHandlerFailureThenCommitsAfterSuccess() throws Exception {
        Consumer<String, String> consumer = mockConsumer(records(validRecord(3)));
        AtomicInteger attempts = new AtomicInteger();
        MessageHandler handler = ignored -> {
            if (attempts.incrementAndGet() == 1) {
                throw new IllegalStateException("temporary");
            }
        };
        ResilientKafkaConsumer loop = loop(consumer, mockProducer(), handler, 1);
        try {
            loop.runOnce();
            awaitCommit(loop, consumer);
            assertThat(attempts).hasValue(2);
        } finally {
            loop.close();
        }
    }

    @Test
    void ignoresCompletionFromRevokedPartition() throws Exception {
        Consumer<String, String> consumer = mockConsumer(records(validRecord(0)));
        ArgumentCaptor<ConsumerRebalanceListener> listener =
                ArgumentCaptor.forClass(ConsumerRebalanceListener.class);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ResilientKafkaConsumer loop = loop(consumer, mockProducer(), ignored -> {
            entered.countDown();
            release.await();
        }, 1);
        try {
            verify(consumer).subscribe(any(java.util.Collection.class), listener.capture());
            listener.getValue().onPartitionsAssigned(List.of(PARTITION));
            loop.runOnce();
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            listener.getValue().onPartitionsRevoked(List.of(PARTITION));
            release.countDown();
            loop.runOnce();
            verify(consumer, times(0)).commitSync(any(Map.class), any(Duration.class));
        } finally {
            release.countDown();
            loop.close();
        }
    }

    private void awaitCommit(ResilientKafkaConsumer loop, Consumer<String, String> consumer)
            throws InterruptedException {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < end) {
            loop.runOnce();
            try {
                verify(consumer).commitSync(any(Map.class), any(Duration.class));
                return;
            } catch (AssertionError pending) {
                Thread.sleep(5);
            }
        }
        verify(consumer).commitSync(any(Map.class), any(Duration.class));
    }

    private ResilientKafkaConsumer loop(Consumer<String, String> consumer,
                                        Producer<String, String> producer,
                                        MessageHandler handler, int workers) {
        return new ResilientKafkaConsumer(consumer, producer,
                new EnvelopeValidator(new ObjectMapper()), handler,
                new ConsumerOptions(workers, workers, 2, Duration.ZERO,
                        Duration.ofMillis(1), WAIT, WAIT), List.of(TOPIC));
    }

    private Consumer<String, String> mockConsumer(ConsumerRecords<String, String> first) {
        Consumer<String, String> consumer = mock(Consumer.class);
        AtomicInteger polls = new AtomicInteger();
        when(consumer.poll(any(Duration.class))).thenAnswer(ignored ->
                polls.getAndIncrement() == 0 ? first : ConsumerRecords.empty());
        when(consumer.assignment()).thenReturn(Set.of(PARTITION));
        when(consumer.paused()).thenReturn(Set.of());
        return consumer;
    }

    private Producer<String, String> mockProducer() {
        Producer<String, String> producer = mock(Producer.class);
        when(producer.send(any())).thenReturn(CompletableFuture.completedFuture(null));
        return producer;
    }

    @SafeVarargs
    private final ConsumerRecords<String, String> records(ConsumerRecord<String, String>... records) {
        return new ConsumerRecords<>(Map.of(PARTITION, List.of(records)));
    }

    private ConsumerRecord<String, String> validRecord(long offset) {
        String deliveryId = "dlv_" + offset;
        String payload = "{\"message_id\":\"msg_8d78be41-a5d2-4faa-ae8d-bbc68c035f13\","
                + "\"message_type\":\"delivery.ready.v1\",\"schema_version\":1,"
                + "\"occurred_at\":\"2026-10-31T10:00:00Z\","
                + "\"organization_id\":\"de568400-3487-45de-a986-c02816a60011\","
                + "\"aggregate_id\":\"" + deliveryId + "\",\"correlation_id\":\"msg_x\","
                + "\"payload\":{\"delivery_id\":\"" + deliveryId + "\"}}";
        return new ConsumerRecord<>(TOPIC, 0, offset, deliveryId, payload);
    }
}

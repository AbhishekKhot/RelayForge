package com.relayforge.analyticsconsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.contracts.EnvelopeValidator;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

class AnalyticsKafkaLifecycleTest {
    private static final String TOPIC = "relayforge.analytics.delivery-facts.v1";

    @Test
    void rejectsFactBatchLargerThanPollBeforeOpeningClients() {
        assertThatThrownBy(() -> new AnalyticsKafkaLifecycle("localhost:9092",
                new EnvelopeValidator(new ObjectMapper()), mock(ClickHouseFactStore.class),
                new SimpleMeterRegistry(), 1, 2)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void commitsBatchOnlyAfterClickHouseAcknowledgesAllFacts() throws Exception {
        var consumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        var producer = new MockProducer<String, String>(true, new StringSerializer(),
                new StringSerializer());
        var store = mock(ClickHouseFactStore.class);
        when(store.insertBatch(any())).thenReturn(List.of(true, true, true));
        var metrics = new SimpleMeterRegistry();
        var lifecycle = new AnalyticsKafkaLifecycle(consumer, producer,
                new EnvelopeValidator(new ObjectMapper()), store, metrics, 3);
        var partition = new TopicPartition(TOPIC, 0);
        consumer.rebalance(List.of(partition));
        consumer.updateBeginningOffsets(Map.of(partition, 0L));
        for (int offset = 0; offset < 3; offset++) {
            UUID factId = UUID.randomUUID();
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset, factId.toString(),
                    validFact(UUID.randomUUID(), factId)));
        }

        lifecycle.runOnce();

        assertThat(consumer.committed(partition).offset()).isEqualTo(3);
        verify(store, times(1)).insertBatch(any());
        assertThat(metrics.get("relayforge.analytics.facts").tag("result", "inserted")
                .counter().count()).isEqualTo(3);
    }

    @Test
    void failedClickHouseBatchSeeksToItsFirstFactWithoutCommitting() throws Exception {
        var consumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        var producer = new MockProducer<String, String>(true, new StringSerializer(),
                new StringSerializer());
        var store = mock(ClickHouseFactStore.class);
        when(store.insertBatch(any())).thenThrow(new IOException("unavailable"));
        var lifecycle = new AnalyticsKafkaLifecycle(consumer, producer,
                new EnvelopeValidator(new ObjectMapper()), store, new SimpleMeterRegistry(), 3);
        var partition = new TopicPartition(TOPIC, 0);
        consumer.rebalance(List.of(partition));
        consumer.updateBeginningOffsets(Map.of(partition, 0L));
        for (int offset = 0; offset < 3; offset++) {
            UUID factId = UUID.randomUUID();
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset, factId.toString(),
                    validFact(UUID.randomUUID(), factId)));
        }

        lifecycle.runOnce();

        assertThat(consumer.committed(partition)).isNull();
        assertThat(consumer.position(partition)).isZero();
        verify(store, times(1)).insertBatch(any());
    }

    @Test
    void failedLaterBatchCommitsOnlyPreviouslyAcknowledgedFacts() throws Exception {
        var consumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        var producer = new MockProducer<String, String>(true, new StringSerializer(),
                new StringSerializer());
        var store = mock(ClickHouseFactStore.class);
        when(store.insertBatch(any())).thenReturn(List.of(true, true))
                .thenThrow(new IOException("unavailable"));
        var lifecycle = new AnalyticsKafkaLifecycle(consumer, producer,
                new EnvelopeValidator(new ObjectMapper()), store, new SimpleMeterRegistry(), 2);
        var partition = new TopicPartition(TOPIC, 0);
        consumer.rebalance(List.of(partition));
        consumer.updateBeginningOffsets(Map.of(partition, 0L));
        for (int offset = 0; offset < 4; offset++) {
            UUID factId = UUID.randomUUID();
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset, factId.toString(),
                    validFact(UUID.randomUUID(), factId)));
        }

        lifecycle.runOnce();

        assertThat(consumer.committed(partition).offset()).isEqualTo(2);
        assertThat(consumer.position(partition)).isEqualTo(2);
        verify(store, times(2)).insertBatch(any());
    }

    @Test
    void malformedMiddleFactSplitsAcknowledgedBatchesAroundQuarantine() throws Exception {
        var consumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        var producer = new MockProducer<String, String>(true, new StringSerializer(),
                new StringSerializer());
        var store = mock(ClickHouseFactStore.class);
        when(store.insertBatch(any())).thenReturn(List.of(true));
        var lifecycle = new AnalyticsKafkaLifecycle(consumer, producer,
                new EnvelopeValidator(new ObjectMapper()), store, new SimpleMeterRegistry(), 3);
        var partition = new TopicPartition(TOPIC, 0);
        consumer.rebalance(List.of(partition));
        consumer.updateBeginningOffsets(Map.of(partition, 0L));
        UUID first = UUID.randomUUID();
        UUID last = UUID.randomUUID();
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, first.toString(),
                validFact(UUID.randomUUID(), first)));
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1, "bad", "invalid-json"));
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 2, last.toString(),
                validFact(UUID.randomUUID(), last)));

        lifecycle.runOnce();

        assertThat(consumer.committed(partition).offset()).isEqualTo(3);
        assertThat(producer.history()).hasSize(1);
        verify(store, times(2)).insertBatch(any());
    }

    @Test
    void commitsProcessedPollAsOneContiguousOffset() throws Exception {
        var consumer = spy(new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST));
        var producer = new MockProducer<String, String>(true, new StringSerializer(),
                new StringSerializer());
        var store = mock(ClickHouseFactStore.class);
        when(store.insert(any())).thenReturn(true);
        var metrics = new SimpleMeterRegistry();
        var lifecycle = new AnalyticsKafkaLifecycle(consumer, producer,
                new EnvelopeValidator(new ObjectMapper()), store, metrics);
        var partition = new TopicPartition(TOPIC, 0);
        consumer.rebalance(List.of(partition));
        consumer.updateBeginningOffsets(Map.of(partition, 0L));
        for (int offset = 0; offset < 3; offset++) {
            UUID factId = UUID.randomUUID();
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset, factId.toString(),
                    validFact(UUID.randomUUID(), factId)));
        }

        lifecycle.runOnce();

        assertThat(consumer.committed(partition).offset()).isEqualTo(3);
        verify(consumer, times(1)).commitSync(any(Map.class), any(Duration.class));
        verify(store, times(3)).insert(any());
        assertThat(metrics.get("relayforge.analytics.kafka.commits").counter().count())
                .isEqualTo(1);
    }

    @Test
    void failedMiddleFactCommitsOnlyEarlierFactsAndReplaysTheTail() throws Exception {
        var consumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        var producer = new MockProducer<String, String>(true, new StringSerializer(),
                new StringSerializer());
        var store = mock(ClickHouseFactStore.class);
        when(store.insert(any())).thenReturn(true).thenThrow(new IOException("unavailable"));
        var metrics = new SimpleMeterRegistry();
        var lifecycle = new AnalyticsKafkaLifecycle(consumer, producer,
                new EnvelopeValidator(new ObjectMapper()), store, metrics);
        var partition = new TopicPartition(TOPIC, 0);
        consumer.rebalance(List.of(partition));
        consumer.updateBeginningOffsets(Map.of(partition, 0L));
        for (int offset = 0; offset < 3; offset++) {
            UUID factId = UUID.randomUUID();
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset, factId.toString(),
                    validFact(UUID.randomUUID(), factId)));
        }

        lifecycle.runOnce();

        assertThat(consumer.committed(partition).offset()).isEqualTo(1);
        assertThat(consumer.position(partition)).isEqualTo(1);
        verify(store, times(2)).insert(any());
        assertThat(metrics.get("relayforge.analytics.facts").tag("result", "retry")
                .counter().count()).isEqualTo(1);
    }

    @Test
    void malformedFactInBatchIsQuarantinedBeforeLaterOffsetCommits() throws Exception {
        var consumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        var producer = new MockProducer<String, String>(true, new StringSerializer(),
                new StringSerializer());
        var store = mock(ClickHouseFactStore.class);
        when(store.insert(any())).thenReturn(true);
        var lifecycle = new AnalyticsKafkaLifecycle(consumer, producer,
                new EnvelopeValidator(new ObjectMapper()), store, new SimpleMeterRegistry());
        var partition = new TopicPartition(TOPIC, 0);
        consumer.rebalance(List.of(partition));
        consumer.updateBeginningOffsets(Map.of(partition, 0L));
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, "bad", "invalid-json"));
        UUID factId = UUID.randomUUID();
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1, factId.toString(),
                validFact(UUID.randomUUID(), factId)));

        lifecycle.runOnce();

        assertThat(producer.history()).hasSize(1);
        assertThat(consumer.committed(partition).offset()).isEqualTo(2);
        verify(store, times(1)).insert(any());
    }

    @Test
    void failedBatchCommitSeeksBackToFirstUncommittedFact() throws Exception {
        var consumer = spy(new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST));
        var producer = new MockProducer<String, String>(true, new StringSerializer(),
                new StringSerializer());
        var store = mock(ClickHouseFactStore.class);
        when(store.insert(any())).thenReturn(true);
        var lifecycle = new AnalyticsKafkaLifecycle(consumer, producer,
                new EnvelopeValidator(new ObjectMapper()), store, new SimpleMeterRegistry());
        var partition = new TopicPartition(TOPIC, 0);
        consumer.rebalance(List.of(partition));
        consumer.updateBeginningOffsets(Map.of(partition, 0L));
        doThrow(new org.apache.kafka.common.errors.TimeoutException("commit unavailable"))
                .when(consumer).commitSync(any(Map.class), any(Duration.class));
        for (int offset = 0; offset < 2; offset++) {
            UUID factId = UUID.randomUUID();
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset, factId.toString(),
                    validFact(UUID.randomUUID(), factId)));
        }

        lifecycle.runOnce();

        assertThat(consumer.committed(partition)).isNull();
        assertThat(consumer.position(partition)).isZero();
        verify(store, times(2)).insert(any());
    }

    private static String validFact(UUID organization, UUID fact) {
        return "{\"message_id\":\"msg_" + UUID.randomUUID() + "\","
                + "\"message_type\":\"analytics.delivery-facts.v1\",\"schema_version\":1,"
                + "\"occurred_at\":\"2026-11-03T10:00:00Z\",\"organization_id\":\""
                + organization + "\",\"aggregate_id\":\"" + fact + "\","
                + "\"correlation_id\":\"test\",\"payload\":{\"organization_id\":\""
                + organization + "\",\"fact_id\":\"" + fact + "\",\"version\":1,"
                + "\"event_date\":\"2026-11-03\",\"fact_type\":\"event.accepted\","
                + "\"status\":\"ACCEPTED\",\"occurred_at\":\"2026-11-03T10:00:00Z\"}}";
    }

    @Test
    void quarantinesMalformedFactWithOnlySourceMetadata() throws Exception {
        var consumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        var producer = new MockProducer<String, String>(true, new StringSerializer(),
                new StringSerializer());
        var lifecycle = new AnalyticsKafkaLifecycle(consumer, producer,
                new EnvelopeValidator(new ObjectMapper()), mock(ClickHouseFactStore.class),
                new SimpleMeterRegistry());
        var partition = new TopicPartition(TOPIC, 0);
        consumer.rebalance(List.of(partition));
        consumer.updateBeginningOffsets(Map.of(partition, 0L));
        String raw = "private-secret-invalid-json";
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, "fact", raw));

        lifecycle.runOnce();

        assertThat(consumer.committed(partition).offset()).isEqualTo(1);
        assertThat(producer.history()).hasSize(1);
        var sent = producer.history().getFirst();
        assertThat(sent.topic()).isEqualTo(TOPIC + ".quarantine");
        var json = new ObjectMapper().readTree(sent.value());
        assertThat(json.path("source_topic").asText()).isEqualTo(TOPIC);
        assertThat(json.path("source_partition").asInt()).isZero();
        assertThat(json.path("source_offset").asLong()).isZero();
        assertThat(json.path("reason").asText()).isEqualTo("INVALID_FACT");
        assertThat(json.path("value_sha256").asText()).isEqualTo(HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(raw.getBytes(StandardCharsets.UTF_8))));
        assertThat(json.path("quarantine_id").asText()).isEqualTo(sent.key());
        assertThat(sent.value()).doesNotContain(raw);
    }

    @Test
    void commitsOnlyAfterClickHouseAcknowledgesFact() throws Exception {
        var consumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        var producer = new MockProducer<String, String>(true, new StringSerializer(),
                new StringSerializer());
        var store = mock(ClickHouseFactStore.class);
        var metrics = new SimpleMeterRegistry();
        var lifecycle = new AnalyticsKafkaLifecycle(consumer, producer,
                new EnvelopeValidator(new ObjectMapper()), store, metrics);
        var partition = new TopicPartition(TOPIC, 0);
        consumer.rebalance(List.of(partition));
        consumer.updateBeginningOffsets(Map.of(partition, 0L));
        UUID organization = UUID.randomUUID();
        UUID fact = UUID.randomUUID();
        String value = "{\"message_id\":\"msg_" + UUID.randomUUID() + "\","
                + "\"message_type\":\"analytics.delivery-facts.v1\",\"schema_version\":1,"
                + "\"occurred_at\":\"2026-11-03T10:00:00Z\",\"organization_id\":\""
                + organization + "\",\"aggregate_id\":\"" + fact + "\","
                + "\"correlation_id\":\"test\",\"payload\":{\"organization_id\":\""
                + organization + "\",\"fact_id\":\"" + fact + "\",\"version\":1,"
                + "\"event_date\":\"2026-11-03\",\"fact_type\":\"event.accepted\","
                + "\"status\":\"ACCEPTED\",\"occurred_at\":\"2026-11-03T10:00:00Z\"}}";
        when(store.insert(any())).thenThrow(new IOException("unavailable")).thenReturn(true);
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, fact.toString(), value));
        lifecycle.runOnce();
        assertThat(consumer.committed(partition)).isNull();
        assertThat(metrics.get("relayforge.clickhouse.insert").tag("result", "error")
                .counter().count()).isEqualTo(1);
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, fact.toString(), value));
        lifecycle.runOnce();
        assertThat(consumer.committed(partition).offset()).isEqualTo(1);
        assertThat(metrics.get("relayforge.clickhouse.insert").tag("result", "success")
                .counter().count()).isEqualTo(1);
        assertThat(metrics.get("relayforge.clickhouse.insert.duration").timer().count())
                .isEqualTo(2);
        assertThat(metrics.get("relayforge.analytics.ingest.lag.seconds").gauge().value())
                .isGreaterThan(0);
    }
}

package com.relayforge.analyticsconsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.contracts.EnvelopeValidator;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
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
    void commitsOnlyAfterClickHouseAcknowledgesFact() throws Exception {
        var consumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        var producer = new MockProducer<String, String>(true, new StringSerializer(),
                new StringSerializer());
        var store = mock(ClickHouseFactStore.class);
        var lifecycle = new AnalyticsKafkaLifecycle(consumer, producer,
                new EnvelopeValidator(new ObjectMapper()), store, new SimpleMeterRegistry());
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
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, fact.toString(), value));
        lifecycle.runOnce();
        assertThat(consumer.committed(partition).offset()).isEqualTo(1);
    }
}

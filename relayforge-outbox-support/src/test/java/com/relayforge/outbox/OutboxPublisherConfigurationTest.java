package com.relayforge.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OutboxPublisherConfigurationTest {
    @Test
    void busyPublisherCanProcessMoreThanOneRowPerPoll() {
        OutboxRepository repository = mock(OutboxRepository.class);
        KafkaOutboxTransport transport = mock(KafkaOutboxTransport.class);
        ClaimedOutboxRow row = new ClaimedOutboxRow(UUID.randomUUID(), UUID.randomUUID(),
                "evt_123", "event.accepted.v1", "relayforge.event.accepted.v1", "evt_123",
                "{\"event_id\":\"evt_123\"}", UUID.randomUUID(), 1, Instant.now());
        when(repository.claim(any(), anyInt(), any()))
                .thenReturn(List.of(row), List.of(row), List.of());
        when(repository.markPublished(row.id(), row.claimToken())).thenReturn(true);

        OutboxPublisher publisher = new OutboxPublisherConfiguration().outboxPublisher(
                repository, transport, new OutboxEnvelope(new ObjectMapper()),
                new MockEnvironment(), new SimpleMeterRegistry());

        assertThat(publisher.publishBatch()).isEqualTo(2);
    }

    @Test
    void configuredPollLimitBoundsOneInvocation() {
        OutboxRepository repository = mock(OutboxRepository.class);
        KafkaOutboxTransport transport = mock(KafkaOutboxTransport.class);
        ClaimedOutboxRow row = new ClaimedOutboxRow(UUID.randomUUID(), UUID.randomUUID(),
                "evt_123", "event.accepted.v1", "relayforge.event.accepted.v1", "evt_123",
                "{\"event_id\":\"evt_123\"}", UUID.randomUUID(), 1, Instant.now());
        when(repository.claim(any(), anyInt(), any())).thenReturn(List.of(row));
        when(repository.markPublished(row.id(), row.claimToken())).thenReturn(true);
        MockEnvironment environment = new MockEnvironment()
                .withProperty("relayforge.outbox.max-rows-per-poll", "3");

        OutboxPublisher publisher = new OutboxPublisherConfiguration().outboxPublisher(
                repository, transport, new OutboxEnvelope(new ObjectMapper()), environment,
                new SimpleMeterRegistry());

        assertThat(publisher.publishBatch()).isEqualTo(3);
    }
}

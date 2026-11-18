package com.relayforge.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.random.RandomGenerator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OutboxPublisherTest {
    private static final UUID ID = UUID.fromString("00000000-0000-0000-0000-000000000001");
    private static final UUID TOKEN = UUID.fromString("00000000-0000-0000-0000-000000000002");
    private final RecordingRepository repository = new RecordingRepository();
    private final RecordingTransport transport = new RecordingTransport();
    private final SimpleMeterRegistry metrics = new SimpleMeterRegistry();

    @Test
    void marksPublishedOnlyAfterAcknowledgement() {
        assertThat(publisher().publishBatch()).isEqualTo(1);
        assertThat(transport.sent).isTrue();
        assertThat(repository.marked).isTrue();
        assertThat(repository.retried).isFalse();
        assertThat(transport.topic).isEqualTo("relayforge.event.accepted.v1");
        assertThat(transport.key).isEqualTo("evt_123");
        assertThat(metrics.get("relayforge.outbox.stage.duration").tag("stage", "claim")
                .timer().count()).isEqualTo(2);
        for (String stage : List.of("publish", "mark")) {
            assertThat(metrics.get("relayforge.outbox.stage.duration").tag("stage", stage)
                    .timer().count()).isEqualTo(1);
        }
    }

    @Test
    void retriesTransportFailureWithoutMarkingOrLoggingFailureMessage() {
        transport.failure = new IllegalStateException("credential-bearer-secret");
        assertThat(publisher().publishBatch()).isZero();
        assertThat(repository.marked).isFalse();
        assertThat(repository.retried).isTrue();
        assertThat(repository.errorCode).isEqualTo("PUBLISH_FAILED");
        assertThat(repository.delay).isEqualTo(Duration.ofMillis(100));
    }

    @Test
    void malformedPayloadIsRetriedWithoutPublishing() {
        repository.row = new ClaimedOutboxRow(ID,
                UUID.fromString("00000000-0000-0000-0000-000000000003"), "evt_123",
                "event.accepted.v1", "relayforge.event.accepted.v1", "evt_123",
                "[1]", TOKEN, 1, Instant.parse("2026-10-31T00:00:00Z"));
        assertThat(publisher().publishBatch()).isZero();
        assertThat(transport.sent).isFalse();
        assertThat(repository.marked).isFalse();
        assertThat(repository.errorCode).isEqualTo("INVALID_ENVELOPE");
    }

    @Test
    void acknowledgementBeforeFailedDatabaseMarkLeavesStableReplay() {
        repository.markFailure = new IllegalStateException("database unavailable");
        assertThatThrownBy(() -> publisher().publishBatch()).isInstanceOf(IllegalStateException.class);
        String firstMessage = transport.message;
        assertThat(transport.sent).isTrue();
        assertThat(repository.retried).isFalse();
        repository.markFailure = null;
        repository.row = row(UUID.randomUUID(), 2);
        assertThat(publisher().publishBatch()).isEqualTo(1);
        assertThat(transport.message).isEqualTo(firstMessage);
    }

    @Test
    void claimsRowsOneAtATimeUntilTheConfiguredPollLimit() {
        OutboxRepository queued = mock(OutboxRepository.class);
        ClaimedOutboxRow first = row(TOKEN, 1);
        ClaimedOutboxRow second = new ClaimedOutboxRow(UUID.randomUUID(), first.organizationId(),
                "evt_456", first.messageType(), first.topic(), "evt_456",
                "{\"event_id\":\"evt_456\"}", UUID.randomUUID(), 1, first.createdAt());
        when(queued.claim("publisher-1", 1, Duration.ofSeconds(30)))
                .thenReturn(List.of(first), List.of(second));
        when(queued.markPublished(first.id(), first.claimToken())).thenReturn(true);
        when(queued.markPublished(second.id(), second.claimToken())).thenReturn(true);
        OutboxPublisher publisher = new OutboxPublisher(queued, transport,
                new OutboxEnvelope(new ObjectMapper()), "publisher-1", 2,
                Duration.ofSeconds(30), new ZeroRandom(), () -> 0, metrics);

        assertThat(publisher.publishBatch()).isEqualTo(2);
        verify(queued, times(2)).claim(eq("publisher-1"), eq(1), eq(Duration.ofSeconds(30)));
    }

    @Test
    void stopsAfterThePollTimeBudgetBeforeClaimingAnotherRow() {
        OutboxRepository queued = mock(OutboxRepository.class);
        when(queued.claim("publisher-1", 1, Duration.ofSeconds(30)))
                .thenReturn(List.of(row(TOKEN, 1)));
        when(queued.markPublished(ID, TOKEN)).thenReturn(true);
        AtomicInteger clockCalls = new AtomicInteger();
        OutboxPublisher publisher = new OutboxPublisher(queued, transport,
                new OutboxEnvelope(new ObjectMapper()), "publisher-1", 100,
                Duration.ofSeconds(30), new ZeroRandom(),
                () -> clockCalls.getAndIncrement() == 0 ? 0 : Duration.ofSeconds(6).toNanos(),
                metrics);

        assertThat(publisher.publishBatch()).isEqualTo(1);
        verify(queued, times(1)).claim("publisher-1", 1, Duration.ofSeconds(30));
    }

    private OutboxPublisher publisher() {
        return new OutboxPublisher(repository, transport, new OutboxEnvelope(new ObjectMapper()),
                "publisher-1", 10, Duration.ofSeconds(30), new ZeroRandom(), metrics);
    }

    private ClaimedOutboxRow row(UUID claimToken, int attempts) {
        return new ClaimedOutboxRow(ID, UUID.fromString("00000000-0000-0000-0000-000000000003"),
                "evt_123", "event.accepted.v1",
                "relayforge.event.accepted.v1", "evt_123", "{\"event_id\":\"evt_123\"}",
                claimToken, attempts, Instant.parse("2026-10-31T00:00:00Z"));
    }

    private static final class RecordingRepository implements OutboxRepository {
        private ClaimedOutboxRow row = new ClaimedOutboxRow(ID,
                UUID.fromString("00000000-0000-0000-0000-000000000003"), "evt_123",
                "event.accepted.v1", "relayforge.event.accepted.v1", "evt_123",
                "{\"event_id\":\"evt_123\"}", TOKEN, 1, Instant.parse("2026-10-31T00:00:00Z"));
        private boolean marked;
        private boolean retried;
        private String errorCode;
        private Duration delay;
        private RuntimeException markFailure;

        @Override
        public List<ClaimedOutboxRow> claim(String workerId, int batchSize, Duration lease) {
            return marked || retried ? List.of() : List.of(row);
        }

        @Override
        public boolean markPublished(UUID id, UUID claimToken) {
            if (markFailure != null) {
                throw markFailure;
            }
            if (!ID.equals(id) || !row.claimToken().equals(claimToken)) {
                throw new AssertionError("Wrong claim identity");
            }
            marked = true;
            return true;
        }

        @Override
        public boolean retry(UUID id, UUID claimToken, Duration retryDelay, String code) {
            retried = true;
            delay = retryDelay;
            errorCode = code;
            return true;
        }
    }

    private static final class RecordingTransport implements OutboxTransport {
        private boolean sent;
        private String topic;
        private String key;
        private String message;
        private RuntimeException failure;

        @Override
        public void publish(String targetTopic, String partitionKey, String envelopeJson) {
            if (failure != null) {
                throw failure;
            }
            topic = targetTopic;
            key = partitionKey;
            message = envelopeJson;
            sent = true;
        }
    }

    private static final class ZeroRandom implements RandomGenerator {
        @Override
        public long nextLong() {
            return 0;
        }
    }
}

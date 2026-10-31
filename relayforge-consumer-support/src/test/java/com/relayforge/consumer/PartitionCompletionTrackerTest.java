package com.relayforge.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class PartitionCompletionTrackerTest {
    private static final TopicPartition PARTITION = new TopicPartition("events", 0);

    @Test
    void commitsOnlyThroughCompletedPrefixEvenWhenFinishesOutOfOrder() {
        PartitionCompletionTracker tracker = new PartitionCompletionTracker();
        tracker.register(PARTITION, 4);
        tracker.register(PARTITION, 7);
        tracker.register(PARTITION, 9);
        tracker.complete(PARTITION, 9);
        tracker.complete(PARTITION, 7);
        assertThat(tracker.committable()).isEmpty();
        tracker.complete(PARTITION, 4);
        assertThat(tracker.committable().get(PARTITION).offset()).isEqualTo(10);
        tracker.committed(PARTITION, 10);
        assertThat(tracker.committable()).isEmpty();
    }

    @Test
    void discardsRevokedPartitionAndRejectsStaleCompletion() {
        PartitionCompletionTracker tracker = new PartitionCompletionTracker();
        tracker.register(PARTITION, 1);
        tracker.revoke(PARTITION);
        assertThrows(IllegalStateException.class, () -> tracker.complete(PARTITION, 1));
        assertThat(tracker.committable()).isEmpty();
    }
}

package com.relayforge.consumer;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

final class PartitionCompletionTracker {
    private final Map<TopicPartition, PartitionState> states = new HashMap<>();

    void register(TopicPartition partition, long offset) {
        PartitionState state = states.computeIfAbsent(partition, ignored -> new PartitionState());
        if (state.offsets.putIfAbsent(offset, false) != null) {
            throw new IllegalStateException("Duplicate offset registration");
        }
    }

    void complete(TopicPartition partition, long offset) {
        PartitionState state = states.get(partition);
        if (state == null || !state.offsets.containsKey(offset)) {
            throw new IllegalStateException("Unassigned offset completion");
        }
        state.offsets.put(offset, true);
    }

    Map<TopicPartition, OffsetAndMetadata> committable() {
        Map<TopicPartition, OffsetAndMetadata> ready = new HashMap<>();
        states.forEach((partition, state) -> {
            long completedThrough = state.committedOffset;
            for (Map.Entry<Long, Boolean> entry : state.offsets.entrySet()) {
                if (!entry.getValue()) {
                    break;
                }
                completedThrough = entry.getKey() + 1;
            }
            if (completedThrough > state.committedOffset) {
                ready.put(partition, new OffsetAndMetadata(completedThrough));
            }
        });
        return ready;
    }

    void committed(TopicPartition partition, long nextOffset) {
        PartitionState state = states.get(partition);
        if (state == null) {
            return;
        }
        state.offsets.headMap(nextOffset, false).clear();
        state.committedOffset = nextOffset;
    }

    void revoke(TopicPartition partition) {
        states.remove(partition);
    }

    private static final class PartitionState {
        private final TreeMap<Long, Boolean> offsets = new TreeMap<>();
        private long committedOffset = -1;
    }
}

package com.relayforge.consumer;

import com.relayforge.contracts.EnvelopeValidator;
import com.relayforge.contracts.InvalidEnvelopeException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

/** A single poll-thread consumer with bounded asynchronous handlers and manual offset commits. */
public final class ResilientKafkaConsumer implements AutoCloseable {
    private final Consumer<String, String> consumer;
    private final Producer<String, String> quarantineProducer;
    private final EnvelopeValidator validator;
    private final MessageHandler handler;
    private final ConsumerOptions options;
    private final ExecutorService workers;
    private final PartitionCompletionTracker tracker = new PartitionCompletionTracker();
    private final BlockingQueue<Completion> completions = new LinkedBlockingQueue<>();
    private final Deque<Pending> pending = new ArrayDeque<>();
    private final Map<TopicPartition, Long> epochs = new HashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean runLoopActive = new AtomicBoolean();
    private int inFlight;
    private boolean stopped;

    public ResilientKafkaConsumer(Consumer<String, String> consumer,
                                  Producer<String, String> quarantineProducer,
                                  EnvelopeValidator validator, MessageHandler handler,
                                  ConsumerOptions options, Collection<String> topics) {
        this.consumer = Objects.requireNonNull(consumer);
        this.quarantineProducer = Objects.requireNonNull(quarantineProducer);
        this.validator = Objects.requireNonNull(validator);
        this.handler = Objects.requireNonNull(handler);
        this.options = Objects.requireNonNull(options);
        if (topics == null || topics.isEmpty()) {
            throw new IllegalArgumentException("At least one topic is required");
        }
        this.workers = Executors.newFixedThreadPool(options.workers());
        consumer.subscribe(topics, new Rebalance());
    }

    /** Polls once, even when every worker is busy, to keep group membership active. */
    public void runOnce() {
        if (!running.get()) {
            return;
        }
        drainCompletions();
        dispatch();
        commitCompleted();
        adjustPause();
        ConsumerRecords<String, String> records = consumer.poll(options.pollTimeout());
        for (ConsumerRecord<String, String> record : records) {
            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            long epoch = epochs.computeIfAbsent(partition, ignored -> 0L);
            tracker.register(partition, record.offset());
            pending.addLast(new Pending(record, partition, epoch));
        }
        dispatch();
        drainCompletions();
        commitCompleted();
        adjustPause();
    }

    public void run() {
        if (!runLoopActive.compareAndSet(false, true)) {
            throw new IllegalStateException("Consumer loop already running");
        }
        try {
            while (running.get()) {
                runOnce();
            }
        } catch (WakeupException waking) {
            if (running.get()) {
                throw waking;
            }
        } finally {
            shutdown();
            runLoopActive.set(false);
        }
    }

    private void dispatch() {
        while (inFlight < options.maxInFlight() && !pending.isEmpty()) {
            Pending next = pending.removeFirst();
            if (!next.epoch().equals(epochs.get(next.partition()))) {
                continue;
            }
            inFlight++;
            workers.execute(() -> process(next));
        }
    }

    private void process(Pending pendingRecord) {
        String failureCode = null;
        for (int attempt = 1; attempt <= options.maxAttempts(); attempt++) {
            try {
                var envelope = validator.validate(pendingRecord.record().topic(),
                        pendingRecord.record().key(), pendingRecord.record().value());
                handler.handle(envelope);
                completions.add(new Completion(pendingRecord, null));
                return;
            } catch (InvalidEnvelopeException invalid) {
                failureCode = invalid.reasonCode();
                break;
            } catch (Error fatal) {
                completions.add(new Completion(pendingRecord, "HANDLER_FATAL"));
                throw fatal;
            } catch (Exception failure) {
                failureCode = "HANDLER_FAILURE";
                if (attempt < options.maxAttempts()) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(options.retryDelay().toMillis());
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        completions.add(new Completion(pendingRecord, "INTERRUPTED"));
                        return;
                    }
                }
            }
        }
        try {
            publishQuarantine(pendingRecord.record(), failureCode);
            completions.add(new Completion(pendingRecord, null));
        } catch (Exception failure) {
            completions.add(new Completion(pendingRecord, "QUARANTINE_FAILURE"));
        }
    }

    private void publishQuarantine(ConsumerRecord<String, String> source, String reason)
            throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
        String identity = source.topic() + ":" + source.partition() + ":" + source.offset();
        String body = "{\"source_topic\":\"" + source.topic() + "\",\"source_partition\":"
                + source.partition() + ",\"source_offset\":" + source.offset()
                + ",\"quarantine_id\":\"" + hash(identity) + "\",\"reason\":\""
                + reason + "\",\"value_sha256\":\"" + hash(source.value()) + "\"}";
        quarantineProducer.send(new ProducerRecord<>(source.topic() + ".quarantine",
                        hash(identity), body))
                .get(options.quarantineTimeout().toMillis(), TimeUnit.MILLISECONDS);
    }

    private static String hash(String value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256").digest(
                    (value == null ? "" : value).getBytes(StandardCharsets.UTF_8));
            return java.util.HexFormat.of().formatHex(bytes);
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("SHA-256 unavailable", unavailable);
        }
    }

    private void drainCompletions() {
        Completion complete;
        while ((complete = completions.poll()) != null) {
            inFlight--;
            Pending processed = complete.pending();
            if (!processed.epoch().equals(epochs.get(processed.partition()))) {
                continue;
            }
            if (complete.failureCode() != null) {
                throw new IllegalStateException(complete.failureCode());
            }
            tracker.complete(processed.partition(), processed.record().offset());
        }
    }

    private void commitCompleted() {
        Map<TopicPartition, OffsetAndMetadata> ready = tracker.committable();
        if (!ready.isEmpty()) {
            consumer.commitSync(ready, options.commitTimeout());
            ready.forEach((partition, offset) -> tracker.committed(partition, offset.offset()));
        }
    }

    private void adjustPause() {
        Set<TopicPartition> assigned = consumer.assignment();
        if (assigned.isEmpty()) {
            return;
        }
        if (inFlight + pending.size() >= options.maxInFlight()) {
            Set<TopicPartition> toPause = new HashSet<>(assigned);
            toPause.removeAll(consumer.paused());
            if (!toPause.isEmpty()) {
                consumer.pause(toPause);
            }
        } else {
            Set<TopicPartition> toResume = new HashSet<>(consumer.paused());
            toResume.retainAll(assigned);
            if (!toResume.isEmpty()) {
                consumer.resume(toResume);
            }
        }
    }

    @Override
    public void close() {
        running.set(false);
        consumer.wakeup();
        if (!runLoopActive.get()) {
            shutdown();
        }
    }

    private synchronized void shutdown() {
        if (stopped) {
            return;
        }
        stopped = true;
        workers.shutdownNow();
        consumer.close(Duration.ofSeconds(5));
        quarantineProducer.close(Duration.ofSeconds(5));
    }

    private final class Rebalance implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            drainCompletions();
            Map<TopicPartition, OffsetAndMetadata> ready = tracker.committable();
            ready.keySet().retainAll(partitions);
            if (!ready.isEmpty()) {
                consumer.commitSync(ready, options.commitTimeout());
            }
            forget(partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            partitions.forEach(partition -> epochs.merge(partition, 1L, Long::sum));
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            forget(partitions);
        }

        private void forget(Collection<TopicPartition> partitions) {
            for (TopicPartition partition : partitions) {
                epochs.merge(partition, 1L, Long::sum);
                tracker.revoke(partition);
            }
            pending.removeIf(item -> partitions.contains(item.partition()));
        }
    }

    private record Pending(ConsumerRecord<String, String> record, TopicPartition partition, Long epoch) { }
    private record Completion(Pending pending, String failureCode) { }
}

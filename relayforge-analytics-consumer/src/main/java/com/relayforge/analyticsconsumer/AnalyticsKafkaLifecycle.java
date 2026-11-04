package com.relayforge.analyticsconsumer;

import com.relayforge.apisupport.ReadinessProbe;
import com.relayforge.consumer.KafkaConsumerSettings;
import com.relayforge.contracts.EnvelopeValidator;
import com.relayforge.contracts.InvalidEnvelopeException;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Gauge;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

public final class AnalyticsKafkaLifecycle implements SmartLifecycle, ReadinessProbe {
    private static final Logger LOG = LoggerFactory.getLogger(AnalyticsKafkaLifecycle.class);
    private static final String TOPIC = "relayforge.analytics.delivery-facts.v1";
    private final Consumer<String, String> consumer;
    private final Producer<String, String> quarantine;
    private final EnvelopeValidator validator;
    private final ClickHouseFactStore store;
    private final MeterRegistry metrics;
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicLong latestFactEpochSeconds = new AtomicLong();
    private final AtomicLong maximumKafkaLag = new AtomicLong();
    private Thread thread;
    private int consecutiveFailures;

    public AnalyticsKafkaLifecycle(String bootstrapServers, EnvelopeValidator validator,
            ClickHouseFactStore store, MeterRegistry metrics) {
        this(new KafkaConsumer<>(consumerSettings(bootstrapServers)),
                new KafkaProducer<>(producerSettings(bootstrapServers)), validator, store, metrics);
    }

    AnalyticsKafkaLifecycle(Consumer<String, String> consumer,
            Producer<String, String> quarantine, EnvelopeValidator validator,
            ClickHouseFactStore store, MeterRegistry metrics) {
        this.consumer = consumer;
        this.quarantine = quarantine;
        this.validator = validator;
        this.store = store;
        this.metrics = metrics;
        Gauge.builder("relayforge.analytics.ingest.lag.seconds", latestFactEpochSeconds,
                value -> value.get() == 0 ? -1 : Math.max(0, Instant.now().getEpochSecond() - value.get()))
                .register(metrics);
        Gauge.builder("relayforge.kafka.consumer.lag.max", maximumKafkaLag, AtomicLong::get)
                .tag("group", "analytics-consumer").register(metrics);
        metrics.timer("relayforge.clickhouse.insert.duration");
        metrics.counter("relayforge.clickhouse.insert", "result", "success");
        metrics.counter("relayforge.clickhouse.insert", "result", "error");
        consumer.subscribe(List.of(TOPIC));
    }

    private static Properties consumerSettings(String servers) {
        Properties settings = KafkaConsumerSettings.properties(servers,
                "relayforge-analytics-consumer", 1, 1, Duration.ofMinutes(5),
                Duration.ofMillis(100), Duration.ofSeconds(3));
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return settings;
    }

    private static Properties producerSettings(String servers) {
        Properties settings = new Properties();
        settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        settings.put(ProducerConfig.ACKS_CONFIG, "all");
        settings.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        settings.put(ProducerConfig.RETRIES_CONFIG, 3);
        settings.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);
        settings.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        settings.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 12000);
        return settings;
    }

    void runOnce() {
        var records = consumer.poll(Duration.ofMillis(100));
        maximumKafkaLag.set(consumer.metrics().entrySet().stream()
                .filter(entry -> entry.getKey().name().equals("records-lag-max")
                        && entry.getKey().group().equals("consumer-fetch-manager-metrics")
                        && !entry.getKey().tags().containsKey("topic"))
                .mapToLong(entry -> Math.max(0, Math.round(
                        ((Number) entry.getValue().metricValue()).doubleValue())))
                .max().orElse(0));
        for (ConsumerRecord<String, String> record : records) {
            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            try {
                var envelope = validator.validate(record.topic(), record.key(), record.value());
                AnalyticsFact fact = AnalyticsFact.parse(envelope);
                long started = System.nanoTime();
                boolean inserted;
                try {
                    inserted = store.insert(fact);
                    metrics.counter("relayforge.clickhouse.insert", "result", "success").increment();
                } catch (Exception unavailable) {
                    metrics.counter("relayforge.clickhouse.insert", "result", "error").increment();
                    throw unavailable;
                } finally {
                    metrics.timer("relayforge.clickhouse.insert.duration").record(
                            Math.max(0, System.nanoTime() - started), TimeUnit.NANOSECONDS);
                }
                latestFactEpochSeconds.accumulateAndGet(fact.occurredAt().getEpochSecond(), Math::max);
                consumer.commitSync(Map.of(partition, new OffsetAndMetadata(record.offset() + 1)),
                        Duration.ofSeconds(3));
                metrics.counter("relayforge.analytics.facts", "result",
                        inserted ? "inserted" : "duplicate").increment();
                LOG.info("Analytics fact stored organizationId={} eventId={} deliveryId={} factId={} "
                                + "result={}", fact.organizationId(), fact.eventId(), fact.deliveryId(),
                        fact.factId(), inserted ? "inserted" : "duplicate");
                consecutiveFailures = 0;
            } catch (InvalidEnvelopeException | IllegalArgumentException invalid) {
                try {
                    quarantine(record);
                    consumer.commitSync(Map.of(partition, new OffsetAndMetadata(record.offset() + 1)),
                            Duration.ofSeconds(3));
                    metrics.counter("relayforge.analytics.facts", "result", "invalid").increment();
                    consecutiveFailures = 0;
                } catch (Exception unavailable) {
                    retry(partition, record.offset());
                }
            } catch (Exception unavailable) {
                if (unavailable instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                retry(partition, record.offset());
            }
        }
    }

    private void quarantine(ConsumerRecord<String, String> record)
            throws InterruptedException, ExecutionException, TimeoutException {
        String identity = record.topic() + ':' + record.partition() + ':' + record.offset();
        String body = "{\"source_topic\":\"" + TOPIC + "\",\"source_partition\":"
                + record.partition() + ",\"source_offset\":" + record.offset()
                + ",\"reason\":\"INVALID_FACT\"}";
        quarantine.send(new ProducerRecord<>(TOPIC + ".quarantine", identity, body))
                .get(3, TimeUnit.SECONDS);
    }

    private void retry(TopicPartition partition, long offset) {
        metrics.counter("relayforge.analytics.facts", "result", "retry").increment();
        consumer.seek(partition, offset);
        consecutiveFailures = Math.min(consecutiveFailures + 1, 7);
        long delay = Math.min(30000, 250L << consecutiveFailures);
        LOG.warn("Analytics insert delayed; retrying after {} ms", delay);
        try {
            Thread.sleep(delay);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void run() {
        try {
            while (running.get()) {
                runOnce();
            }
        } catch (WakeupException waking) {
            if (running.get()) {
                LOG.error("Analytics Kafka poll stopped unexpectedly");
            }
        } catch (RuntimeException fatal) {
            LOG.error("Analytics Kafka consumer stopped unexpectedly");
        } finally {
            consumer.close(Duration.ofSeconds(5));
            quarantine.close(Duration.ofSeconds(5));
            running.set(false);
        }
    }

    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            thread = Thread.ofPlatform().daemon(true).name("relayforge-analytics-consumer")
                    .start(this::run);
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            consumer.wakeup();
            if (thread != null) {
                thread.interrupt();
                try {
                    thread.join(10000);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @Override
    public void stop(Runnable callback) {
        try {
            stop();
        } finally {
            callback.run();
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public boolean available() {
        return running.get() && thread != null && thread.isAlive();
    }
}

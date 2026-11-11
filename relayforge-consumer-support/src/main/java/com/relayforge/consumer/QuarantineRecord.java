package com.relayforge.consumer;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

/** Metadata-only record for a source message that cannot be processed. */
public final class QuarantineRecord {
    private QuarantineRecord() { }

    public static ProducerRecord<String, String> from(ConsumerRecord<String, String> source,
                                                       String reason) {
        if (reason == null || !reason.matches("[A-Z_]{1,64}")) {
            throw new IllegalArgumentException("Invalid quarantine reason");
        }
        String identity = source.topic() + ":" + source.partition() + ":" + source.offset();
        String key = hash(identity);
        String body = "{\"source_topic\":\"" + source.topic() + "\",\"source_partition\":"
                + source.partition() + ",\"source_offset\":" + source.offset()
                + ",\"quarantine_id\":\"" + key + "\",\"reason\":\""
                + reason + "\",\"value_sha256\":\"" + hash(source.value()) + "\"}";
        return new ProducerRecord<>(source.topic() + ".quarantine", key, body);
    }

    private static String hash(String value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256").digest(
                    (value == null ? "" : value).getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(bytes);
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("SHA-256 unavailable", unavailable);
        }
    }
}

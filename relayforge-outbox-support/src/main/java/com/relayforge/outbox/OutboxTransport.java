package com.relayforge.outbox;

@FunctionalInterface
public interface OutboxTransport {
    void publish(String topic, String partitionKey, String envelopeJson) throws Exception;
}

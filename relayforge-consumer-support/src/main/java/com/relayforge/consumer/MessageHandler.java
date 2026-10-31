package com.relayforge.consumer;

import com.relayforge.contracts.ValidatedEnvelope;

@FunctionalInterface
public interface MessageHandler {
    void handle(ValidatedEnvelope envelope) throws Exception;
}

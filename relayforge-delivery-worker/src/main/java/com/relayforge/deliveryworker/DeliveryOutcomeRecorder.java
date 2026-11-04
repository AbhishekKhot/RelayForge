package com.relayforge.deliveryworker;

import com.relayforge.deliveryworker.retry.RetryPolicy;
import java.time.Duration;

@FunctionalInterface
public interface DeliveryOutcomeRecorder {
    void record(RetryPolicy.Outcome outcome, Duration elapsed);
}

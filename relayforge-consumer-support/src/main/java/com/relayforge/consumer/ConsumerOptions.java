package com.relayforge.consumer;

import java.time.Duration;

public record ConsumerOptions(int workers, int maxInFlight, int maxAttempts,
                              Duration retryDelay, Duration pollTimeout,
                              Duration commitTimeout, Duration quarantineTimeout) {
    public ConsumerOptions {
        if (workers < 1 || maxInFlight < workers || maxAttempts < 1
                || retryDelay == null || retryDelay.isNegative()
                || pollTimeout == null || pollTimeout.isNegative() || pollTimeout.isZero()
                || commitTimeout == null || commitTimeout.isNegative() || commitTimeout.isZero()
                || quarantineTimeout == null || quarantineTimeout.isNegative() || quarantineTimeout.isZero()) {
            throw new IllegalArgumentException("Invalid consumer options");
        }
    }
}

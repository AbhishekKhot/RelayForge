package com.relayforge.deliveryworker.retry;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Objects;
import java.util.function.DoubleSupplier;

public final class RetryPolicy {
    private final Clock clock;
    private final DoubleSupplier random;
    private final Settings settings;

    public RetryPolicy(Clock clock, DoubleSupplier random, Settings settings) {
        this.clock = Objects.requireNonNull(clock);
        this.random = Objects.requireNonNull(random);
        this.settings = Objects.requireNonNull(settings);
    }

    public Decision decide(int status, Failure failure, int attemptNumber, int maxAttempts,
            Instant createdAt, Instant eventExpiresAt, String retryAfter) {
        if (status < 0 || status > 599 || attemptNumber < 1 || maxAttempts < 1
                || attemptNumber > maxAttempts || createdAt == null || eventExpiresAt == null
                || failure == null) {
            throw new IllegalArgumentException("Invalid delivery outcome");
        }
        if (status >= 200 && status < 300 && failure == Failure.NONE) {
            return new Decision(Outcome.SUCCEEDED, null, null);
        }
        boolean retryable = failure == Failure.NETWORK || failure == Failure.NONE
                && (status == 408 || status == 429 || status >= 500);
        String errorCode = failure == Failure.SECURITY ? "SECURITY_REJECTED"
                : failure == Failure.TLS ? "TLS_REJECTED"
                : failure == Failure.NETWORK ? "NETWORK_ERROR"
                : retryable ? "HTTP_RETRYABLE" : "HTTP_TERMINAL";
        if (!retryable || attemptNumber >= maxAttempts) {
            return new Decision(Outcome.DEAD_LETTERED, null, errorCode);
        }
        Instant now = clock.instant();
        Instant horizon = createdAt.plus(settings.maxDeliveryAge());
        if (eventExpiresAt.isBefore(horizon)) {
            horizon = eventExpiresAt;
        }
        long capMillis = settings.baseDelay().toMillis();
        long maxMillis = settings.maxDelay().toMillis();
        for (int retry = 1; retry < attemptNumber && capMillis < maxMillis; retry++) {
            capMillis = Math.min(maxMillis, capMillis * 2);
        }
        double draw = random.getAsDouble();
        if (!(draw >= 0 && draw < 1)) {
            throw new IllegalStateException("Invalid retry random value");
        }
        Duration delay = Duration.ofMillis((long) (draw * capMillis));
        if ((status == 429 || status == 503) && failure == Failure.NONE) {
            Duration requested = retryAfter(retryAfter, now);
            if (requested.compareTo(delay) > 0) {
                delay = requested;
            }
        }
        Instant next = now.plus(delay);
        return !next.isBefore(horizon)
                ? new Decision(Outcome.DEAD_LETTERED, null, errorCode)
                : new Decision(Outcome.RETRY_SCHEDULED, next, errorCode);
    }

    private Duration retryAfter(String raw, Instant now) {
        if (raw == null || raw.length() > 80 || raw.isBlank()) {
            return Duration.ZERO;
        }
        String value = raw.trim();
        Duration requested;
        try {
            if (value.matches("[0-9]{1,18}")) {
                requested = Duration.ofSeconds(Long.parseLong(value));
            } else {
                requested = Duration.between(now,
                        ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant());
            }
        } catch (DateTimeParseException | ArithmeticException invalid) {
            return Duration.ZERO;
        }
        if (requested.isNegative()) {
            return Duration.ZERO;
        }
        return requested.compareTo(settings.maxRetryAfter()) > 0
                ? settings.maxRetryAfter() : requested;
    }

    public enum Failure {
        NONE, NETWORK, SECURITY, TLS
    }

    public enum Outcome {
        SUCCEEDED, RETRY_SCHEDULED, DEAD_LETTERED
    }

    public record Decision(Outcome outcome, Instant nextAttemptAt, String errorCode) {
    }

    public record Settings(Duration baseDelay, Duration maxDelay, Duration maxRetryAfter,
            Duration maxDeliveryAge) {
        public Settings {
            if (baseDelay == null || maxDelay == null || maxRetryAfter == null
                    || maxDeliveryAge == null || baseDelay.isNegative() || baseDelay.isZero()
                    || maxDelay.compareTo(baseDelay) < 0 || maxRetryAfter.isNegative()
                    || maxDeliveryAge.isNegative() || maxDeliveryAge.isZero()
                    || maxDelay.compareTo(Duration.ofDays(1)) > 0
                    || maxRetryAfter.compareTo(Duration.ofDays(1)) > 0
                    || maxDeliveryAge.compareTo(Duration.ofDays(90)) > 0) {
                throw new IllegalArgumentException("Invalid retry settings");
            }
        }
    }
}

package com.relayforge.analyticsapi;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public record AnalyticsRange(Instant from, Instant to, Interval interval) {
    private static final Duration MAXIMUM_RANGE = Duration.ofDays(31);

    public static AnalyticsRange parse(String from, String to, String interval) {
        try {
            if (from == null || to == null || !from.endsWith("Z") || !to.endsWith("Z")) {
                throw new IllegalArgumentException("UTC range required");
            }
            Instant start = Instant.parse(from);
            Instant end = Instant.parse(to);
            if (start.getNano() % 1_000_000 != 0 || end.getNano() % 1_000_000 != 0
                    || interval == null) {
                throw new IllegalArgumentException("Millisecond UTC range and interval required");
            }
            Duration duration = Duration.between(start, end);
            if (duration.isNegative() || duration.isZero() || duration.compareTo(MAXIMUM_RANGE) > 0) {
                throw new IllegalArgumentException("Range must be within 31 days");
            }
            Interval parsed = switch (interval) {
                case "hour" -> Interval.HOUR;
                case "day" -> Interval.DAY;
                default -> throw new IllegalArgumentException("Unsupported interval");
            };
            return new AnalyticsRange(start, end, parsed);
        } catch (DateTimeException | IllegalArgumentException exception) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid analytics range");
        }
    }

    public LocalDate startDate() {
        return from.atZone(ZoneOffset.UTC).toLocalDate();
    }

    public LocalDate endDate() {
        return to.minusNanos(1).atZone(ZoneOffset.UTC).toLocalDate();
    }

    public enum Interval {
        HOUR, DAY
    }
}

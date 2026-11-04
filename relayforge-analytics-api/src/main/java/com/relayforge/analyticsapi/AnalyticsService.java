package com.relayforge.analyticsapi;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiConsumer;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public final class AnalyticsService {
    private final AnalyticsRepository repository;
    private final BiConsumer<String, Duration> queryDuration;

    public AnalyticsService(AnalyticsRepository repository) {
        this(repository, (query, duration) -> { });
    }

    public AnalyticsService(AnalyticsRepository repository, BiConsumer<String, Duration> queryDuration) {
        this.repository = Objects.requireNonNull(repository);
        this.queryDuration = Objects.requireNonNull(queryDuration);
    }

    public AnalyticsRepository.Overview overview(UUID organizationId, String from, String to) {
        AnalyticsRange range = AnalyticsRange.parse(from, to, "day");
        long started = System.nanoTime();
        try {
            return repository.overview(organizationId, range);
        } catch (IOException failure) {
            throw new AnalyticsUnavailableException();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AnalyticsUnavailableException();
        } finally {
            queryDuration.accept("overview", Duration.ofNanos(Math.max(0, System.nanoTime() - started)));
        }
    }

    public AnalyticsRepository.TimeSeries timeseries(UUID organizationId, String from, String to,
            String interval, String endpointId) {
        AnalyticsRange range = AnalyticsRange.parse(from, to, interval);
        UUID endpoint = null;
        if (endpointId != null) {
            try {
                endpoint = UUID.fromString(endpointId);
                if (!endpoint.toString().equals(endpointId)) {
                    throw new IllegalArgumentException("Invalid endpoint ID");
                }
            } catch (IllegalArgumentException failure) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid endpoint ID");
            }
        }
        long started = System.nanoTime();
        try {
            return repository.timeseries(organizationId, range, endpoint);
        } catch (IOException failure) {
            throw new AnalyticsUnavailableException();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AnalyticsUnavailableException();
        } finally {
            queryDuration.accept("timeseries", Duration.ofNanos(Math.max(0, System.nanoTime() - started)));
        }
    }
}

package com.relayforge.analyticsapi;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public final class AnalyticsService {
    private final AnalyticsRepository repository;

    public AnalyticsService(AnalyticsRepository repository) {
        this.repository = Objects.requireNonNull(repository);
    }

    public AnalyticsRepository.Overview overview(UUID organizationId, String from, String to) {
        AnalyticsRange range = AnalyticsRange.parse(from, to, "day");
        try {
            return repository.overview(organizationId, range);
        } catch (IOException failure) {
            throw new AnalyticsUnavailableException();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AnalyticsUnavailableException();
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
        try {
            return repository.timeseries(organizationId, range, endpoint);
        } catch (IOException failure) {
            throw new AnalyticsUnavailableException();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AnalyticsUnavailableException();
        }
    }
}

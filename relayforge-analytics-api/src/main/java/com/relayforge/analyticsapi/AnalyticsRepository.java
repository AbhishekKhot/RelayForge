package com.relayforge.analyticsapi;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface AnalyticsRepository {
    Overview overview(UUID organizationId, AnalyticsRange range)
            throws IOException, InterruptedException;

    TimeSeries timeseries(UUID organizationId, AnalyticsRange range, UUID endpointId)
            throws IOException, InterruptedException;

    record Overview(long eventsAccepted, long deliveriesCreated, long deliveriesSucceeded,
                    long deliveriesDeadLettered, long deliveriesRetryScheduled, Instant dataAsOf) {
    }

    record Point(Instant bucket, long deliveriesCreated, long deliveriesSucceeded,
                 long deliveriesDeadLettered, long deliveriesRetryScheduled) {
    }

    record TimeSeries(List<Point> points, Instant dataAsOf) {
    }
}

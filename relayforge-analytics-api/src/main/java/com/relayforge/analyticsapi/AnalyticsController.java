package com.relayforge.analyticsapi;

import com.relayforge.apisupport.ApiError;
import com.relayforge.apisupport.RequestIdFilter;
import jakarta.servlet.http.HttpServletRequest;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@ConditionalOnProperty(name = "relayforge.clickhouse.url")
public final class AnalyticsController {
    private final AnalyticsAuthenticator authenticator;
    private final AnalyticsService service;

    public AnalyticsController(AnalyticsAuthenticator authenticator, AnalyticsService service) {
        this.authenticator = authenticator;
        this.service = service;
    }

    @GetMapping("/v1/analytics/overview")
    public Map<String, Object> overview(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to, HttpServletRequest request) {
        UUID organization = authenticator.authenticate(authorization);
        var result = service.overview(organization, from, to);
        Map<String, Object> data = Map.of("events_accepted", result.eventsAccepted(),
                "deliveries_created", result.deliveriesCreated(),
                "deliveries_succeeded", result.deliveriesSucceeded(),
                "deliveries_dead_lettered", result.deliveriesDeadLettered(),
                "deliveries_retry_scheduled", result.deliveriesRetryScheduled());
        return response(data, result.dataAsOf(), request);
    }

    @GetMapping("/v1/analytics/deliveries/timeseries")
    public Map<String, Object> timeseries(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to,
            @RequestParam(required = false) String interval,
            @RequestParam(value = "endpoint_id", required = false) String endpointId,
            HttpServletRequest request) {
        UUID organization = authenticator.authenticate(authorization);
        var result = service.timeseries(organization, from, to, interval, endpointId);
        List<Map<String, Object>> points = result.points().stream().map(point -> Map.<String, Object>of(
                "bucket", point.bucket().toString(),
                "deliveries_created", point.deliveriesCreated(),
                "deliveries_succeeded", point.deliveriesSucceeded(),
                "deliveries_dead_lettered", point.deliveriesDeadLettered(),
                "deliveries_retry_scheduled", point.deliveriesRetryScheduled())).toList();
        Map<String, Object> data = Map.of("interval", interval, "points", points);
        return response(data, result.dataAsOf(), request);
    }

    @ExceptionHandler(AnalyticsUnavailableException.class)
    public ResponseEntity<ApiError> unavailable(AnalyticsUnavailableException failure,
            HttpServletRequest request) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(new ApiError(
                "ANALYTICS_TEMPORARILY_UNAVAILABLE", "Analytics is temporarily unavailable",
                RequestIdFilter.current(request)));
    }

    private Map<String, Object> response(Object data, java.time.Instant asOf,
            HttpServletRequest request) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("data", data);
        response.put("data_as_of", asOf == null ? null : asOf.toString());
        response.put("request_id", RequestIdFilter.current(request));
        return response;
    }
}

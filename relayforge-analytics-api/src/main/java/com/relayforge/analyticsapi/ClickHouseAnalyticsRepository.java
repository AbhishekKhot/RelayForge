package com.relayforge.analyticsapi;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public final class ClickHouseAnalyticsRepository implements AnalyticsRepository {
    private static final String TABLE = "relayforge_analytics.delivery_facts FINAL";
    private static final int MAXIMUM_POINTS = 745;
    private static final int MAXIMUM_RESPONSE_BYTES = 262144;
    private final URI endpoint;
    private final String username;
    private final String password;
    private final ObjectMapper json;
    private final HttpClient client;

    public ClickHouseAnalyticsRepository(URI base, String username, String password,
            ObjectMapper json, HttpClient client) {
        if (base == null || !("http".equals(base.getScheme()) || "https".equals(base.getScheme()))
                || base.getHost() == null || base.getPort() < 1 || base.getRawUserInfo() != null
                || base.getRawQuery() != null || base.getRawFragment() != null
                || base.getRawPath() != null && !base.getRawPath().isEmpty()
                || username == null || username.isBlank() || password == null || password.isBlank()) {
            throw new IllegalArgumentException("Invalid ClickHouse connection configuration");
        }
        this.endpoint = URI.create(base + "/?wait_end_of_query=1&max_execution_time=3"
                + "&max_result_rows=746&result_overflow_mode=throw&max_result_bytes=262144"
                + "&output_format_json_quote_64bit_integers=0");
        this.username = username;
        this.password = password;
        this.json = Objects.requireNonNull(json);
        this.client = Objects.requireNonNull(client);
    }

    @Override
    public Overview overview(UUID organizationId, AnalyticsRange range)
            throws IOException, InterruptedException {
        String sql = "SELECT countIf(fact_type = 'event.accepted') AS events_accepted, "
                + "countIf(fact_type = 'delivery.created') AS deliveries_created, "
                + "countIf(fact_type = 'delivery.succeeded') AS deliveries_succeeded, "
                + "countIf(fact_type = 'delivery.dead_lettered') AS deliveries_dead_lettered, "
                + "countIf(fact_type = 'delivery.retry_scheduled') AS deliveries_retry_scheduled, "
                + "maxOrNull(toUnixTimestamp64Milli(ingested_at)) AS data_as_of_ms FROM "
                + TABLE + where(organizationId, range, null) + " FORMAT JSONEachRow";
        List<JsonNode> rows = query(sql);
        if (rows.size() != 1) {
            throw new IOException("Invalid ClickHouse overview response");
        }
        JsonNode row = rows.getFirst();
        return new Overview(number(row, "events_accepted"), number(row, "deliveries_created"),
                number(row, "deliveries_succeeded"), number(row, "deliveries_dead_lettered"),
                number(row, "deliveries_retry_scheduled"), timestamp(row, "data_as_of_ms"));
    }

    @Override
    public TimeSeries timeseries(UUID organizationId, AnalyticsRange range, UUID endpointId)
            throws IOException, InterruptedException {
        String bucket = range.interval() == AnalyticsRange.Interval.HOUR
                ? "toStartOfHour(occurred_at)" : "toStartOfDay(occurred_at)";
        String sql = "SELECT toUnixTimestamp(" + bucket + ") * 1000 AS bucket_ms, "
                + "countIf(fact_type = 'delivery.created') AS deliveries_created, "
                + "countIf(fact_type = 'delivery.succeeded') AS deliveries_succeeded, "
                + "countIf(fact_type = 'delivery.dead_lettered') AS deliveries_dead_lettered, "
                + "countIf(fact_type = 'delivery.retry_scheduled') AS deliveries_retry_scheduled, "
                + "max(toUnixTimestamp64Milli(ingested_at)) AS data_as_of_ms FROM "
                + TABLE + where(organizationId, range, endpointId)
                + " AND startsWith(fact_type, 'delivery.') GROUP BY bucket_ms "
                + "ORDER BY bucket_ms LIMIT 746 FORMAT JSONEachRow";
        List<JsonNode> rows = query(sql);
        if (rows.size() > MAXIMUM_POINTS) {
            throw new IOException("ClickHouse timeseries exceeded row limit");
        }
        List<Point> points = new ArrayList<>(rows.size());
        Instant asOf = null;
        for (JsonNode row : rows) {
            points.add(new Point(Instant.ofEpochMilli(number(row, "bucket_ms")),
                    number(row, "deliveries_created"), number(row, "deliveries_succeeded"),
                    number(row, "deliveries_dead_lettered"),
                    number(row, "deliveries_retry_scheduled")));
            Instant rowAsOf = timestamp(row, "data_as_of_ms");
            if (asOf == null || rowAsOf.isAfter(asOf)) {
                asOf = rowAsOf;
            }
        }
        return new TimeSeries(List.copyOf(points), asOf);
    }

    private String where(UUID organizationId, AnalyticsRange range, UUID endpointId) {
        String predicate = " WHERE organization_id = toUUID('" + organizationId + "')"
                + " AND event_date >= '" + range.startDate() + "'"
                + " AND event_date <= '" + range.endDate() + "'"
                + " AND occurred_at >= fromUnixTimestamp64Milli(" + range.from().toEpochMilli() + ")"
                + " AND occurred_at < fromUnixTimestamp64Milli(" + range.to().toEpochMilli() + ")";
        return endpointId == null ? predicate : predicate + " AND endpoint_id = toUUID('"
                + endpointId + "')";
    }

    private List<JsonNode> query(String sql) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(endpoint).timeout(Duration.ofSeconds(4))
                .header("X-ClickHouse-User", username)
                .header("X-ClickHouse-Key", password)
                .header("Content-Type", "text/plain; charset=UTF-8")
                .POST(HttpRequest.BodyPublishers.ofString(sql, StandardCharsets.UTF_8)).build();
        HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        try (InputStream body = response.body()) {
            byte[] bytes = body.readNBytes(MAXIMUM_RESPONSE_BYTES + 1);
            if (bytes.length > MAXIMUM_RESPONSE_BYTES || response.statusCode() != 200) {
                throw new IOException("ClickHouse query failed");
            }
            List<JsonNode> rows = new ArrayList<>();
            for (String line : new String(bytes, StandardCharsets.UTF_8).split("\\R")) {
                if (!line.isBlank()) {
                    rows.add(json.readTree(line));
                    if (rows.size() > MAXIMUM_POINTS + 1) {
                        throw new IOException("ClickHouse query exceeded row limit");
                    }
                }
            }
            return rows;
        }
    }

    private long number(JsonNode row, String field) throws IOException {
        JsonNode value = row.get(field);
        if (value == null || !value.isIntegralNumber() || !value.canConvertToLong()) {
            throw new IOException("Invalid ClickHouse analytics response");
        }
        return value.longValue();
    }

    private Instant timestamp(JsonNode row, String field) throws IOException {
        JsonNode value = row.get(field);
        if (value == null || value.isNull()) {
            return null;
        }
        return Instant.ofEpochMilli(number(row, field));
    }
}

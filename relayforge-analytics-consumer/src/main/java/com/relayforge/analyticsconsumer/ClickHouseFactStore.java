package com.relayforge.analyticsconsumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public final class ClickHouseFactStore {
    private static final String COLUMNS = "organization_id,event_date,fact_id,version,event_id,"
            + "delivery_id,endpoint_id,replay_generation,fact_type,status,attempt_number,"
            + "http_status,duration_ms,error_code,occurred_at";
    private final URI endpoint;
    private final String username;
    private final String password;
    private final ObjectMapper json;
    private final HttpClient client;
    private final MeterRegistry metrics;

    public ClickHouseFactStore(URI base, String username, String password, ObjectMapper json,
            HttpClient client, MeterRegistry metrics) {
        if (base == null || !("http".equals(base.getScheme()) || "https".equals(base.getScheme()))
                || base.getHost() == null || base.getPort() < 1 || base.getRawUserInfo() != null
                || base.getRawQuery() != null || base.getRawFragment() != null
                || base.getRawPath() != null && !base.getRawPath().isEmpty()
                || username == null || username.isBlank() || password == null || password.isBlank()) {
            throw new IllegalArgumentException("Invalid ClickHouse connection configuration");
        }
        this.endpoint = URI.create(base + "/?wait_end_of_query=1&async_insert=0"
                + "&output_format_json_quote_64bit_integers=0&max_execution_time=3");
        this.username = username;
        this.password = password;
        this.json = Objects.requireNonNull(json);
        this.client = Objects.requireNonNull(client);
        this.metrics = Objects.requireNonNull(metrics);
        metrics.timer("relayforge.clickhouse.fact.stage.duration", "stage", "lookup");
        metrics.timer("relayforge.clickhouse.fact.stage.duration", "stage", "insert");
    }

    public boolean insert(AnalyticsFact fact) throws IOException, InterruptedException {
        String expected = fact.json(json);
        String query = "SELECT " + COLUMNS + " FROM relayforge_analytics.delivery_facts "
                + "WHERE organization_id = '" + fact.organizationId() + "' AND event_date = '"
                + fact.eventDate() + "' AND fact_id = '" + fact.factId() + "' AND version = "
                + fact.version() + " FORMAT JSONEachRow";
        String rows = sendTimed(query, 65536, "lookup");
        for (String line : rows.split("\\R")) {
            if (line.isBlank()) {
                continue;
            }
            JsonNode existing;
            try {
                existing = json.readTree(line);
            } catch (IOException malformed) {
                throw new IOException("ClickHouse returned an invalid fact row", malformed);
            }
            if (!json.readTree(expected).equals(existing)) {
                throw new IllegalArgumentException("Conflicting analytics fact version");
            }
            return false;
        }
        sendTimed("INSERT INTO relayforge_analytics.delivery_facts (" + COLUMNS
                + ") FORMAT JSONEachRow\n" + expected + "\n", 4096, "insert");
        return true;
    }

    public List<Boolean> insertBatch(List<AnalyticsFact> facts)
            throws IOException, InterruptedException {
        if (facts == null || facts.size() > 32 || facts.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("Invalid analytics fact batch");
        }
        if (facts.isEmpty()) {
            return List.of();
        }
        Map<FactKey, JsonNode> expected = new LinkedHashMap<>();
        Map<FactKey, Integer> firstIndex = new LinkedHashMap<>();
        for (int index = 0; index < facts.size(); index++) {
            AnalyticsFact fact = facts.get(index);
            FactKey key = key(fact);
            JsonNode value = json.readTree(fact.json(json));
            JsonNode previous = expected.putIfAbsent(key, value);
            if (previous != null && !previous.equals(value)) {
                throw new IllegalArgumentException("Conflicting analytics fact version");
            }
            firstIndex.putIfAbsent(key, index);
        }
        StringBuilder query = new StringBuilder("SELECT ").append(COLUMNS)
                .append(" FROM relayforge_analytics.delivery_facts WHERE ")
                .append("(organization_id,event_date,fact_id,version) IN (");
        boolean first = true;
        for (FactKey key : expected.keySet()) {
            if (!first) {
                query.append(',');
            }
            query.append("(toUUID('").append(key.organizationId()).append("'),toDate('")
                    .append(key.eventDate()).append("'),toUUID('").append(key.factId())
                    .append("'),").append(key.version()).append(')');
            first = false;
        }
        query.append(") FORMAT JSONEachRow");
        String existingRows = sendTimed(query.toString(), 1024 * 1024, "lookup");
        for (String line : existingRows.split("\\R")) {
            if (line.isBlank()) {
                continue;
            }
            JsonNode existing = json.readTree(line);
            FactKey key;
            try {
                key = new FactKey(UUID.fromString(existing.path("organization_id").asText()),
                        LocalDate.parse(existing.path("event_date").asText()),
                        UUID.fromString(existing.path("fact_id").asText()),
                        existing.path("version").asLong(-1));
            } catch (RuntimeException malformed) {
                throw new IOException("ClickHouse returned an invalid fact row", malformed);
            }
            JsonNode requested = expected.get(key);
            if (requested == null) {
                throw new IOException("ClickHouse returned an unexpected fact row");
            }
            if (!requested.equals(existing)) {
                throw new IllegalArgumentException("Conflicting analytics fact version");
            }
            firstIndex.remove(key);
        }
        List<Boolean> inserted = new ArrayList<>(java.util.Collections.nCopies(facts.size(), false));
        if (!firstIndex.isEmpty()) {
            StringBuilder insert = new StringBuilder("INSERT INTO relayforge_analytics.delivery_facts (")
                    .append(COLUMNS).append(") FORMAT JSONEachRow\n");
            for (FactKey key : firstIndex.keySet()) {
                insert.append(expected.get(key)).append('\n');
                inserted.set(firstIndex.get(key), true);
            }
            sendTimed(insert.toString(), 4096, "insert");
        }
        return inserted;
    }

    private static FactKey key(AnalyticsFact fact) {
        return new FactKey(fact.organizationId(), fact.eventDate(), fact.factId(), fact.version());
    }

    private record FactKey(UUID organizationId, LocalDate eventDate, UUID factId, long version) { }

    private String sendTimed(String query, int maximumResponseBytes, String stage)
            throws IOException, InterruptedException {
        var sample = Timer.start(metrics);
        try {
            return send(query, maximumResponseBytes);
        } finally {
            sample.stop(metrics.timer("relayforge.clickhouse.fact.stage.duration", "stage", stage));
        }
    }

    private String send(String query, int maximumResponseBytes) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(endpoint)
                .timeout(Duration.ofSeconds(4))
                .header("X-ClickHouse-User", username)
                .header("X-ClickHouse-Key", password)
                .header("Content-Type", "text/plain; charset=UTF-8")
                .POST(HttpRequest.BodyPublishers.ofString(query, StandardCharsets.UTF_8))
                .build();
        HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        try (InputStream body = response.body()) {
            byte[] value = body.readNBytes(maximumResponseBytes + 1);
            if (value.length > maximumResponseBytes || response.statusCode() != 200) {
                throw new IOException("ClickHouse query was not acknowledged");
            }
            return new String(value, StandardCharsets.UTF_8);
        }
    }
}

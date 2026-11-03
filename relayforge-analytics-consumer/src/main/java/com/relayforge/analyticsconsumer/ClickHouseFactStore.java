package com.relayforge.analyticsconsumer;

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
import java.util.Objects;

public final class ClickHouseFactStore {
    private static final String COLUMNS = "organization_id,event_date,fact_id,version,event_id,"
            + "delivery_id,endpoint_id,replay_generation,fact_type,status,attempt_number,"
            + "http_status,duration_ms,error_code,occurred_at";
    private final URI endpoint;
    private final String username;
    private final String password;
    private final ObjectMapper json;
    private final HttpClient client;

    public ClickHouseFactStore(URI base, String username, String password, ObjectMapper json,
            HttpClient client) {
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
    }

    public boolean insert(AnalyticsFact fact) throws IOException, InterruptedException {
        String expected = fact.json(json);
        String query = "SELECT " + COLUMNS + " FROM relayforge_analytics.delivery_facts "
                + "WHERE organization_id = '" + fact.organizationId() + "' AND event_date = '"
                + fact.eventDate() + "' AND fact_id = '" + fact.factId() + "' AND version = "
                + fact.version() + " FORMAT JSONEachRow";
        String rows = send(query, 65536);
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
        send("INSERT INTO relayforge_analytics.delivery_facts (" + COLUMNS
                + ") FORMAT JSONEachRow\n" + expected + "\n", 4096);
        return true;
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

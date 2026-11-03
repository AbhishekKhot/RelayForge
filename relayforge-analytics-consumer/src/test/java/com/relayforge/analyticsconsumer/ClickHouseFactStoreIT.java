package com.relayforge.analyticsconsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.contracts.ValidatedEnvelope;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

class ClickHouseFactStoreIT {
    private final ObjectMapper json = new ObjectMapper();

    @Test
    void duplicateAndCorrectionSelectOneLatestRowWithNullFields() throws Exception {
        try (var clickhouse = new GenericContainer<>(
                DockerImageName.parse("clickhouse/clickhouse-server:26.8.3.105"))
                .withEnv("CLICKHOUSE_USER", "relayforge")
                .withEnv("CLICKHOUSE_PASSWORD", "integration-secret")
                .withEnv("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
                .withExposedPorts(8123)
                .waitingFor(Wait.forHttp("/ping").forPort(8123).forStatusCode(200))) {
            clickhouse.start();
            URI base = URI.create("http://" + clickhouse.getHost() + ':'
                    + clickhouse.getMappedPort(8123));
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
            String ddl = Files.readString(Path.of("../deploy/clickhouse/01-schema.sql"));
            for (String statement : ddl.split(";")) {
                if (!statement.isBlank()) {
                    assertThat(query(client, base, statement)).isEmpty();
                }
            }
            assertThat(query(client, base, "SHOW CREATE TABLE relayforge_analytics.delivery_facts"))
                    .contains("ReplacingMergeTree(version)", "toYYYYMM(event_date)",
                            "ORDER BY (organization_id, event_date, fact_id)");
            UUID organization = UUID.randomUUID();
            UUID factId = UUID.randomUUID();
            ClickHouseFactStore store = new ClickHouseFactStore(base, "relayforge",
                    "integration-secret", json, client);
            AnalyticsFact first = fact(organization, factId, 1, "FAILED", 500);
            assertThat(store.insert(first)).isTrue();
            assertThat(store.insert(first)).isFalse();
            AnalyticsFact correction = fact(organization, factId, 2, "UNKNOWN", null);
            assertThat(store.insert(correction)).isTrue();
            String selected = query(client, base, "SELECT version, status, http_status FROM "
                    + "relayforge_analytics.delivery_facts FINAL WHERE organization_id = '"
                    + organization + "' AND fact_id = '" + factId + "' FORMAT JSONEachRow");
            assertThat(selected.lines()).hasSize(1);
            var row = json.readTree(selected);
            assertThat(row.path("version").asLong()).isEqualTo(2);
            assertThat(row.path("status").asText()).isEqualTo("UNKNOWN");
            assertThat(row.path("http_status").isNull()).isTrue();
            assertThatThrownBy(() -> store.insert(fact(organization, factId, 2,
                    "SUCCEEDED", 204))).isInstanceOf(IllegalArgumentException.class);
            query(client, base, "INSERT INTO relayforge_analytics.delivery_facts "
                    + "(organization_id, event_date, fact_id, version, fact_type, status, occurred_at) "
                    + "SELECT toUUID('" + organization + "'), toDate('2026-11-03'), "
                    + "generateUUIDv4(), 1, 'delivery.created', 'PENDING', "
                    + "toDateTime64('2026-11-03 10:00:00', 3, 'UTC') FROM numbers(1000)");
            long started = System.nanoTime();
            String total = query(client, base, "SELECT count() FROM "
                    + "relayforge_analytics.delivery_facts FINAL WHERE organization_id = '"
                    + organization + "' AND event_date = '2026-11-03'");
            long elapsedMillis = Duration.ofNanos(System.nanoTime() - started).toMillis();
            assertThat(total).isEqualTo("1001");
            System.out.println("H2 FINAL 1001-row query duration_ms=" + elapsedMillis);
        }
    }

    private AnalyticsFact fact(UUID organization, UUID factId, long version,
            String status, Integer httpStatus) throws Exception {
        String body = "{\"organization_id\":\"" + organization + "\",\"fact_id\":\""
                + factId + "\",\"version\":" + version + ",\"event_date\":\"2026-11-03\","
                + "\"fact_type\":\"delivery.attempt.finished\",\"status\":\"" + status
                + "\",\"http_status\":" + (httpStatus == null ? "null" : httpStatus)
                + ",\"occurred_at\":\"2026-11-03T10:00:00Z\"}";
        return AnalyticsFact.parse(new ValidatedEnvelope("msg_" + UUID.randomUUID(),
                "analytics.delivery-facts.v1", 1, Instant.parse("2026-11-03T10:00:00Z"),
                organization, factId.toString(), "correlation", json.readTree(body)));
    }

    private String query(HttpClient client, URI base, String sql) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(base).timeout(Duration.ofSeconds(10))
                .header("X-ClickHouse-User", "relayforge")
                .header("X-ClickHouse-Key", "integration-secret")
                .POST(HttpRequest.BodyPublishers.ofString(sql)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return response.body().trim();
    }
}

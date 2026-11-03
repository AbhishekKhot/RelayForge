package com.relayforge.analyticsapi;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
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

class ClickHouseAnalyticsRepositoryIT {
    @Test
    void aggregatesLatestFactsOnlyWithinTenantAndUtcWindow() throws Exception {
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
            for (String statement : Files.readString(Path.of("../deploy/clickhouse/01-schema.sql"))
                    .split(";")) {
                if (!statement.isBlank()) {
                    query(client, base, statement);
                }
            }
            UUID tenant = UUID.randomUUID();
            UUID other = UUID.randomUUID();
            UUID endpoint = UUID.randomUUID();
            UUID fact = UUID.randomUUID();
            insert(client, base, tenant, endpoint, UUID.randomUUID(), 1, "event.accepted",
                    "2026-11-03 10:00:00");
            insert(client, base, tenant, endpoint, UUID.randomUUID(), 1, "delivery.created",
                    "2026-11-03 10:05:00");
            insert(client, base, tenant, endpoint, fact, 1, "delivery.succeeded",
                    "2026-11-03 10:10:00");
            insert(client, base, tenant, endpoint, fact, 2, "delivery.succeeded",
                    "2026-11-03 10:10:00");
            insert(client, base, other, endpoint, UUID.randomUUID(), 1,
                    "delivery.dead_lettered", "2026-11-03 10:15:00");
            ClickHouseAnalyticsRepository repository = new ClickHouseAnalyticsRepository(base,
                    "relayforge", "integration-secret", new ObjectMapper(), client);
            AnalyticsRange range = AnalyticsRange.parse("2026-11-03T10:00:00Z",
                    "2026-11-03T11:00:00Z", "hour");
            var overview = repository.overview(tenant, range);
            assertThat(overview.eventsAccepted()).isEqualTo(1);
            assertThat(overview.deliveriesCreated()).isEqualTo(1);
            assertThat(overview.deliveriesSucceeded()).isEqualTo(1);
            assertThat(overview.deliveriesDeadLettered()).isZero();
            assertThat(overview.dataAsOf()).isNotNull();
            var series = repository.timeseries(tenant, range, endpoint);
            assertThat(series.points()).hasSize(1);
            assertThat(series.points().getFirst().bucket()).isEqualTo(
                    Instant.parse("2026-11-03T10:00:00Z"));
            assertThat(series.points().getFirst().deliveriesSucceeded()).isEqualTo(1);
            assertThat(series.dataAsOf()).isNotNull();
            assertThat(repository.timeseries(tenant, range, UUID.randomUUID()).points()).isEmpty();
            assertThat(repository.overview(other, range).deliveriesDeadLettered()).isEqualTo(1);
            assertThat(repository.overview(tenant, AnalyticsRange.parse(
                    "2026-11-03T10:10:00Z", "2026-11-03T10:11:00Z", "hour"))
                    .deliveriesCreated()).isZero();
        }
    }

    private void insert(HttpClient client, URI base, UUID tenant, UUID endpoint,
            UUID fact, int version, String type, String occurred) throws Exception {
        query(client, base, "INSERT INTO relayforge_analytics.delivery_facts "
                + "(organization_id,event_date,fact_id,version,endpoint_id,fact_type,status,occurred_at) "
                + "VALUES ('" + tenant + "','2026-11-03','" + fact + "'," + version
                + ",'" + endpoint + "','" + type + "','SUCCEEDED','" + occurred + "')");
    }

    private void query(HttpClient client, URI base, String sql) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(base).timeout(Duration.ofSeconds(10))
                .header("X-ClickHouse-User", "relayforge")
                .header("X-ClickHouse-Key", "integration-secret")
                .POST(HttpRequest.BodyPublishers.ofString(sql)).build();
        assertThat(client.send(request, HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(200);
    }
}

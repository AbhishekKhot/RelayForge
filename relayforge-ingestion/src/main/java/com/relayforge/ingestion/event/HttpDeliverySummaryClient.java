package com.relayforge.ingestion.event;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalDate;
import java.util.Set;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public final class HttpDeliverySummaryClient implements DeliverySummaryClient {
    private static final Set<String> STATUSES = Set.of("PENDING", "SELECTING", "ROUTING", "ROUTED",
            "NO_SUBSCRIBERS");
    private final HttpClient http;
    private final ObjectMapper json;
    private final URI baseUrl;
    private final String credential;

    public HttpDeliverySummaryClient(URI baseUrl, String credential, ObjectMapper json) {
        if (baseUrl == null || !baseUrl.isAbsolute() || baseUrl.getHost() == null
                || baseUrl.getRawUserInfo() != null || baseUrl.getRawQuery() != null
                || baseUrl.getRawFragment() != null || credential == null || credential.length() < 32) {
            throw new IllegalStateException("Delivery summary configuration is unavailable");
        }
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2))
                .followRedirects(HttpClient.Redirect.NEVER).build();
        this.baseUrl = baseUrl;
        this.credential = credential;
        this.json = json;
    }

    @Override
    public DeliverySummary read(UUID organizationId, LocalDate acceptedDay, UUID eventId) {
        URI target = baseUrl.resolve("/internal/v1/events/" + eventId + "/summary?organization_id="
                + organizationId + "&accepted_day=" + acceptedDay);
        HttpRequest request = HttpRequest.newBuilder(target).timeout(Duration.ofSeconds(3))
                .header("X-RelayForge-Event-Summary-Credential", credential).GET().build();
        try {
            HttpResponse<InputStream> response = http.send(request, HttpResponse.BodyHandlers.ofInputStream());
            byte[] body;
            try (InputStream stream = response.body()) {
                body = stream.readNBytes(4097);
            }
            if (response.statusCode() != 200 || body.length > 4096
                    || response.headers().firstValue("Content-Type").stream()
                    .noneMatch(value -> value.toLowerCase(java.util.Locale.ROOT).startsWith("application/json"))) {
                throw unavailable();
            }
            JsonNode root = json.readTree(body);
            if (root == null || !root.isObject()) {
                throw unavailable();
            }
            String status = root.path("status").asText("");
            int targets = count(root, "target_count");
            int deliveries = count(root, "delivery_count");
            int succeeded = count(root, "succeeded_count");
            int deadLettered = count(root, "dead_lettered_count");
            if (!STATUSES.contains(status) || succeeded + deadLettered > deliveries) {
                throw unavailable();
            }
            return new DeliverySummary(status, targets, deliveries, succeeded, deadLettered);
        } catch (IOException | InterruptedException failure) {
            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw unavailable();
        }
    }

    private int count(JsonNode root, String field) {
        JsonNode value = root.path(field);
        if (!value.isIntegralNumber() || !value.canConvertToInt()
                || value.intValue() < 0 || value.intValue() > 1000000) {
            throw unavailable();
        }
        return value.intValue();
    }

    private ResponseStatusException unavailable() {
        return new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
    }
}

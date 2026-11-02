package com.relayforge.router.routing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.contracts.ValidatedEnvelope;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;

public final class HttpEventReferenceVerifier {
    private final HttpClient http;
    private final URI baseUrl;
    private final String credential;
    private final ObjectMapper json;
    private final String deliveryShardId;

    public HttpEventReferenceVerifier(URI baseUrl, String credential, ObjectMapper json,
            String deliveryShardId) {
        if (baseUrl == null || !baseUrl.isAbsolute() || baseUrl.getHost() == null
                || !("http".equals(baseUrl.getScheme()) || "https".equals(baseUrl.getScheme()))
                || baseUrl.getRawUserInfo() != null || baseUrl.getRawQuery() != null
                || baseUrl.getRawFragment() != null || credential == null
                || credential.length() < 32 || credential.length() > 256
                || deliveryShardId == null || deliveryShardId.isBlank()) {
            throw new IllegalStateException("Router event verification configuration is unavailable");
        }
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2))
                .followRedirects(HttpClient.Redirect.NEVER).build();
        this.baseUrl = baseUrl;
        this.credential = credential;
        this.json = Objects.requireNonNull(json);
        this.deliveryShardId = deliveryShardId;
    }

    public VerifiedEvent verify(ValidatedEnvelope envelope) {
        if (envelope == null || !"event.accepted.v1".equals(envelope.messageType())) {
            throw invalid();
        }
        JsonNode payload = envelope.payload();
        String locator = payload.path("event_id").asText("");
        if (!locator.matches("evt_[A-Za-z0-9_-]{1,296}")
                || !deliveryShardId.equals(payload.path("delivery_shard_id").asText())) {
            throw invalid();
        }
        URI target = baseUrl.resolve("/internal/v1/events/" + locator + "/routing?organization_id="
                + envelope.organizationId());
        HttpRequest request = HttpRequest.newBuilder(target).timeout(Duration.ofSeconds(3))
                .header("X-RelayForge-Router-Event-Credential", credential).GET().build();
        try {
            HttpResponse<InputStream> response = http.send(request, HttpResponse.BodyHandlers.ofInputStream());
            byte[] body;
            try (InputStream stream = response.body()) {
                body = stream.readNBytes(4097);
            }
            if (response.statusCode() != 200 || body.length > 4096
                    || response.headers().firstValue("Content-Type").stream()
                    .noneMatch(value -> value.toLowerCase(Locale.ROOT).startsWith("application/json"))) {
                throw invalid();
            }
            JsonNode found = json.readTree(body);
            if (found == null || !found.isObject()
                    || !envelope.organizationId().equals(uuid(found, "organization_id"))
                    || !uuid(found, "event_id").toString().equals(envelope.aggregateId())
                    || !text(found, "event_type").equals(text(payload, "event_type"))
                    || !text(found, "payload_hash").equals(text(payload, "payload_hash"))
                    || !text(found, "payload_hash").matches("[0-9a-f]{64}")
                    || !text(found, "accepted_day").equals(text(payload, "accepted_day"))
                    || !text(found, "accepted_at").equals(text(payload, "accepted_at"))) {
                throw invalid();
            }
            LocalDate day = LocalDate.parse(text(found, "accepted_day"));
            Instant acceptedAt = Instant.parse(text(found, "accepted_at"));
            return new VerifiedEvent(envelope.organizationId(), uuid(found, "event_id"), locator, day,
                    acceptedAt, text(found, "event_type"), text(payload, "ingestion_shard_id"),
                    deliveryShardId);
        } catch (IOException unavailable) {
            throw invalid();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw invalid();
        } catch (IllegalArgumentException malformed) {
            throw invalid();
        }
    }

    private UUID uuid(JsonNode node, String field) {
        String value = text(node, field);
        UUID parsed = UUID.fromString(value);
        if (!parsed.toString().equals(value)) {
            throw invalid();
        }
        return parsed;
    }

    private String text(JsonNode node, String field) {
        JsonNode value = node.path(field);
        if (!value.isTextual() || value.asText().isBlank() || value.asText().length() > 300) {
            throw invalid();
        }
        return value.asText();
    }

    private IllegalStateException invalid() {
        return new IllegalStateException("Event reference verification failed");
    }
}

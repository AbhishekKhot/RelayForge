package com.relayforge.deliveryworker.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;

public final class DeliveryReferenceClient {
    private final HttpClient http;
    private final ObjectMapper json;
    private final URI control;
    private final URI ingestion;
    private final String configCredential;
    private final String payloadCredential;

    public DeliveryReferenceClient(URI control, URI ingestion, String configCredential,
            String payloadCredential, ObjectMapper json) {
        this.control = validBase(control);
        this.ingestion = validBase(ingestion);
        this.configCredential = validCredential(configCredential);
        this.payloadCredential = validCredential(payloadCredential);
        this.json = Objects.requireNonNull(json);
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2))
                .followRedirects(HttpClient.Redirect.NEVER).build();
    }

    public JsonNode endpoint(UUID organizationId, UUID endpointId) throws IOException {
        Objects.requireNonNull(organizationId);
        Objects.requireNonNull(endpointId);
        URI target = control.resolve("/internal/v1/organizations/" + organizationId
                + "/endpoints/" + endpointId + "/delivery");
        return fetch(target, "X-RelayForge-Delivery-Config-Credential", configCredential,
                65536, true);
    }

    public JsonNode payload(UUID organizationId, String locator) throws IOException {
        Objects.requireNonNull(organizationId);
        if (locator == null || !locator.matches("evt_[A-Za-z0-9_-]{1,296}")) {
            throw new IllegalArgumentException("Invalid event locator");
        }
        URI target = ingestion.resolve("/internal/v1/events/" + locator
                + "/payload?organization_id=" + organizationId);
        return fetch(target, "X-RelayForge-Delivery-Payload-Credential", payloadCredential,
                300000, false);
    }

    private JsonNode fetch(URI target, String header, String credential, int maximum,
                           boolean endpoint) throws IOException {
        HttpRequest request = HttpRequest.newBuilder(target).timeout(Duration.ofSeconds(5))
                .header(header, credential).GET().build();
        try {
            HttpResponse<InputStream> response = http.send(request, HttpResponse.BodyHandlers.ofInputStream());
            byte[] body;
            try (InputStream stream = response.body()) {
                if (endpoint && response.statusCode() == 404) {
                    throw new EndpointNotActiveException();
                }
                body = stream.readNBytes(maximum + 1);
            }
            if (response.statusCode() != 200 || body.length > maximum
                    || response.headers().firstValue("Content-Type").stream()
                    .noneMatch(value -> value.toLowerCase(java.util.Locale.ROOT)
                            .startsWith("application/json"))) {
                throw new IOException("Delivery reference is unavailable");
            }
            JsonNode value = json.readTree(body);
            if (value == null || !value.isObject()) {
                throw new IOException("Delivery reference is invalid");
            }
            return value;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Delivery reference was interrupted", interrupted);
        }
    }

    private URI validBase(URI base) {
        if (base == null || !base.isAbsolute() || base.getHost() == null
                || !("http".equals(base.getScheme()) || "https".equals(base.getScheme()))
                || base.getRawUserInfo() != null || base.getRawQuery() != null
                || base.getRawFragment() != null) {
            throw new IllegalStateException("Delivery reference URL is unavailable");
        }
        return base;
    }

    private String validCredential(String value) {
        if (value == null || value.length() < 32 || value.length() > 256) {
            throw new IllegalStateException("Delivery reference credential is unavailable");
        }
        return value;
    }

    public static final class EndpointNotActiveException extends IOException {
        public EndpointNotActiveException() {
            super("Endpoint is unavailable for delivery");
        }
    }
}

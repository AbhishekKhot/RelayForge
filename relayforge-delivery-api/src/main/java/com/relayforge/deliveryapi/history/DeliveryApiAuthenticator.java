package com.relayforge.deliveryapi.history;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public final class DeliveryApiAuthenticator {
    private final HttpClient http;
    private final URI readCheckUrl;
    private final URI replayCheckUrl;
    private final ObjectMapper json;

    public DeliveryApiAuthenticator(URI controlUrl, ObjectMapper json) {
        if (controlUrl == null || !controlUrl.isAbsolute() || controlUrl.getHost() == null
                || !("http".equals(controlUrl.getScheme()) || "https".equals(controlUrl.getScheme()))
                || controlUrl.getRawUserInfo() != null || controlUrl.getRawQuery() != null
                || controlUrl.getRawFragment() != null) {
            throw new IllegalStateException("Control-plane URL is unavailable");
        }
        this.readCheckUrl = controlUrl.resolve("/v1/auth/check?scope=deliveries:read");
        this.replayCheckUrl = controlUrl.resolve("/v1/auth/check?scope=deliveries:replay");
        this.json = Objects.requireNonNull(json);
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2))
                .followRedirects(HttpClient.Redirect.NEVER).build();
    }

    public UUID authenticate(String authorization) {
        return authenticate(authorization, readCheckUrl);
    }

    public UUID authenticateReplay(String authorization) {
        return authenticate(authorization, replayCheckUrl);
    }

    private UUID authenticate(String authorization, URI checkUrl) {
        if (authorization == null || !authorization.startsWith("Bearer ")
                || authorization.length() > 135) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
        }
        HttpRequest request = HttpRequest.newBuilder(checkUrl).timeout(Duration.ofSeconds(3))
                .header("Authorization", authorization).GET().build();
        try {
            HttpResponse<InputStream> response = http.send(request, HttpResponse.BodyHandlers.ofInputStream());
            byte[] body;
            try (InputStream stream = response.body()) {
                body = stream.readNBytes(4097);
            }
            if (response.statusCode() == 401) {
                throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
            }
            if (response.statusCode() == 403) {
                throw new ResponseStatusException(HttpStatus.FORBIDDEN);
            }
            if (response.statusCode() != 200 || body.length > 4096
                    || response.headers().firstValue("Content-Type").stream()
                    .noneMatch(value -> value.toLowerCase(Locale.ROOT).startsWith("application/json"))) {
                throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
            }
            JsonNode parsed = json.readTree(body);
            if (parsed == null || !parsed.isObject()) {
                throw new IllegalArgumentException("Invalid authorization response");
            }
            String value = parsed.path("organization_id").asText("");
            UUID organization = UUID.fromString(value);
            if (!organization.toString().equals(value)) {
                throw new IllegalArgumentException("Invalid organization identifier");
            }
            return organization;
        } catch (IOException | IllegalArgumentException failure) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
        }
    }
}

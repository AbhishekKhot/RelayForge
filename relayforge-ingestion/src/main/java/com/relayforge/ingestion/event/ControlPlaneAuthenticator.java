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
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public final class ControlPlaneAuthenticator implements OrganizationAuthenticator {
    private final HttpClient http;
    private final ObjectMapper json;
    private final URI checkUrl;

    public ControlPlaneAuthenticator(URI controlUrl, ObjectMapper json) {
        if (controlUrl == null || !controlUrl.isAbsolute() || controlUrl.getHost() == null
                || controlUrl.getRawUserInfo() != null || controlUrl.getRawQuery() != null
                || controlUrl.getRawFragment() != null) {
            throw new IllegalStateException("A control-plane URL is required");
        }
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2))
                .followRedirects(HttpClient.Redirect.NEVER).build();
        this.json = json;
        this.checkUrl = controlUrl.resolve("/v1/auth/check");
    }

    @Override
    public AuthenticatedOrganization authenticate(String authorization) {
        if (authorization == null || !authorization.startsWith("Bearer ") || authorization.length() > 135) {
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
            if (response.statusCode() != 200 || body.length > 4096) {
                throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
            }
            JsonNode parsed = json.readTree(body);
            UUID organizationId = UUID.fromString(parsed.path("organization_id").asText());
            int limit = parsed.path("events_per_minute").asInt(-1);
            if (limit < 1 || limit > 60000) {
                throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
            }
            return new AuthenticatedOrganization(organizationId, limit);
        } catch (IOException | InterruptedException | IllegalArgumentException failure) {
            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
        }
    }
}

package com.relayforge.router.routing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public final class HttpConfigurationSnapshotClient implements ConfigurationSnapshotClient {
    private static final int PAGE_LIMIT = 100;
    private static final int MAX_ENDPOINTS = 20000;
    private static final int MAX_SUBSCRIPTIONS = 100000;
    private static final int MAX_PAGE_BYTES = 65536;
    private final HttpClient http;
    private final URI baseUrl;
    private final String credential;
    private final ObjectMapper json;

    public HttpConfigurationSnapshotClient(URI baseUrl, String credential, ObjectMapper json) {
        if (baseUrl == null || !baseUrl.isAbsolute() || baseUrl.getHost() == null
                || !("http".equals(baseUrl.getScheme()) || "https".equals(baseUrl.getScheme()))
                || baseUrl.getRawUserInfo() != null || baseUrl.getRawQuery() != null
                || baseUrl.getRawFragment() != null || credential == null
                || credential.length() < 32 || credential.length() > 256) {
            throw invalid();
        }
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2))
                .followRedirects(HttpClient.Redirect.NEVER).build();
        this.baseUrl = baseUrl;
        this.credential = credential;
        this.json = Objects.requireNonNull(json);
    }

    @Override
    public ConfigurationSnapshot load(UUID organizationId) {
        Objects.requireNonNull(organizationId);
        List<ConfigurationSnapshot.Endpoint> endpoints = new ArrayList<>();
        List<ConfigurationSnapshot.Subscription> subscriptions = new ArrayList<>();
        long revision = loadEndpoints(organizationId, endpoints);
        long subscriptionRevision = loadSubscriptions(organizationId, subscriptions);
        Page lastCheck = fetch(organizationId, "endpoints", null);
        if (revision != subscriptionRevision || revision != lastCheck.revision()) {
            throw invalid();
        }
        return new ConfigurationSnapshot(revision, endpoints, subscriptions);
    }

    private long loadEndpoints(UUID organizationId, List<ConfigurationSnapshot.Endpoint> result) {
        UUID cursor = null;
        Set<UUID> seen = new HashSet<>();
        Long revision = null;
        do {
            Page page = fetch(organizationId, "endpoints", cursor);
            revision = checkRevision(revision, page.revision());
            for (JsonNode row : page.rows()) {
                UUID id = uuid(row.path("endpoint_id"));
                if (!seen.add(id)) {
                    throw invalid();
                }
                result.add(new ConfigurationSnapshot.Endpoint(id,
                        status(row.path("status"), Set.of("ACTIVE", "PAUSED", "DISABLED")),
                        positiveLong(row.path("version"))));
            }
            if (result.size() > MAX_ENDPOINTS) {
                throw invalid();
            }
            cursor = next(cursor, page.cursor(), seen);
        } while (cursor != null);
        return revision;
    }

    private long loadSubscriptions(UUID organizationId, List<ConfigurationSnapshot.Subscription> result) {
        UUID cursor = null;
        Set<UUID> seen = new HashSet<>();
        Long revision = null;
        do {
            Page page = fetch(organizationId, "subscriptions", cursor);
            revision = checkRevision(revision, page.revision());
            for (JsonNode row : page.rows()) {
                UUID id = uuid(row.path("subscription_id"));
                if (!seen.add(id)) {
                    throw invalid();
                }
                JsonNode filter = row.path("filter_config");
                if (!filter.isNull() && !filter.isTextual()) {
                    throw invalid();
                }
                result.add(new ConfigurationSnapshot.Subscription(id, uuid(row.path("endpoint_id")),
                        text(row.path("event_type"), 200),
                        status(row.path("status"), Set.of("ACTIVE", "PAUSED", "DELETED")),
                        positiveLong(row.path("version")), filter.isNull() ? null : filter.asText()));
            }
            if (result.size() > MAX_SUBSCRIPTIONS) {
                throw invalid();
            }
            cursor = next(cursor, page.cursor(), seen);
        } while (cursor != null);
        return revision;
    }

    private Page fetch(UUID organizationId, String kind, UUID after) {
        String path = "/internal/v1/organizations/" + organizationId + "/configuration/" + kind
                + "?limit=" + PAGE_LIMIT + (after == null ? "" : "&after=" + after);
        HttpRequest request = HttpRequest.newBuilder(baseUrl.resolve(path))
                .timeout(Duration.ofSeconds(3))
                .header("X-RelayForge-Router-Config-Credential", credential).GET().build();
        try {
            HttpResponse<InputStream> response = http.send(request, HttpResponse.BodyHandlers.ofInputStream());
            byte[] body;
            try (InputStream stream = response.body()) {
                body = stream.readNBytes(MAX_PAGE_BYTES + 1);
            }
            if (response.statusCode() != 200 || body.length > MAX_PAGE_BYTES
                    || response.headers().firstValue("Content-Type").stream()
                    .noneMatch(value -> value.toLowerCase(Locale.ROOT).startsWith("application/json"))) {
                throw invalid();
            }
            JsonNode root = json.readTree(body);
            if (root == null || !root.isObject() || !root.path("source_revision").isIntegralNumber()
                    || !root.path("source_revision").canConvertToLong()
                    || root.path("source_revision").longValue() < 0
                    || !root.path("data").isArray() || root.path("data").size() > PAGE_LIMIT) {
                throw invalid();
            }
            JsonNode next = root.path("next_cursor");
            UUID cursor = next.isNull() ? null : uuid(next);
            return new Page(root.path("source_revision").longValue(), root.path("data"), cursor);
        } catch (IOException unavailable) {
            throw invalid();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw invalid();
        } catch (IllegalArgumentException malformed) {
            throw invalid();
        }
    }

    private long checkRevision(Long previous, long current) {
        if (previous != null && previous != current) {
            throw invalid();
        }
        return current;
    }

    private UUID next(UUID previous, UUID cursor, Set<UUID> seen) {
        if (cursor != null && (cursor.equals(previous) || !seen.contains(cursor))) {
            throw invalid();
        }
        return cursor;
    }

    private UUID uuid(JsonNode value) {
        String raw = text(value, 36);
        UUID parsed = UUID.fromString(raw);
        if (!parsed.toString().equals(raw)) {
            throw invalid();
        }
        return parsed;
    }

    private long positiveLong(JsonNode value) {
        if (!value.isIntegralNumber() || !value.canConvertToLong() || value.longValue() < 1) {
            throw invalid();
        }
        return value.longValue();
    }

    private String status(JsonNode value, Set<String> allowed) {
        String status = text(value, 24);
        if (!allowed.contains(status)) {
            throw invalid();
        }
        return status;
    }

    private String text(JsonNode value, int maximum) {
        if (!value.isTextual() || value.asText().isBlank() || value.asText().length() > maximum) {
            throw invalid();
        }
        return value.asText();
    }

    private IllegalStateException invalid() {
        return new IllegalStateException("Configuration snapshot unavailable");
    }

    private record Page(long revision, JsonNode rows, UUID cursor) {
    }
}

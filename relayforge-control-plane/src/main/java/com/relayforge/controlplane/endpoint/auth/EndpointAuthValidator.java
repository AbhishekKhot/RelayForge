package com.relayforge.controlplane.endpoint.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.relayforge.outbound.Destination;
import com.relayforge.outbound.DestinationPolicy;
import com.relayforge.outbound.InvalidDestinationException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public final class EndpointAuthValidator {
    private static final Pattern NAME = Pattern.compile("[A-Za-z][A-Za-z0-9_-]{0,79}");
    private static final Pattern JSON_PATH = Pattern.compile(
            "\\$((\\.[A-Za-z_][A-Za-z0-9_]*)|(\\[[0-9]+\\]))+");
    private static final Pattern SCOPE = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}");
    private static final Set<String> BLOCKED_HEADERS = Set.of("host", "connection", "content-length",
            "transfer-encoding", "proxy-authorization", "te", "trailer", "upgrade", "expect",
            "cookie", "set-cookie", "keep-alive", "content-type");

    private final DestinationPolicy destinationPolicy;

    public EndpointAuthValidator(DestinationPolicy destinationPolicy) {
        this.destinationPolicy = destinationPolicy;
    }

    public ValidatedAuthentication validate(JsonNode request) {
        if (request == null || !request.isObject() || request.toString().getBytes(StandardCharsets.UTF_8).length > 16384) {
            throw invalid();
        }
        String type = text(request, "type", 40);
        switch (type) {
            case "NONE" -> fields(request, Set.of("type"));
            case "STATIC_HEADER", "STATIC_API_KEY" -> validateStaticHeader(request);
            case "BEARER" -> {
                fields(request, Set.of("type", "token"));
                text(request, "token", 4096);
            }
            case "BASIC" -> {
                fields(request, Set.of("type", "username", "password"));
                String username = text(request, "username", 512);
                if (username.indexOf(':') >= 0) {
                    throw invalid();
                }
                text(request, "password", 4096);
            }
            case "TOKEN_EXCHANGE" -> validateExchange(request);
            case "CLIENT_CREDENTIALS" -> validateClientCredentials(request);
            default -> throw invalid();
        }
        return new ValidatedAuthentication(type, type.equals("NONE") ? null : request.toString());
    }

    private void validateStaticHeader(JsonNode request) {
        fields(request, Set.of("type", "header_name", "value"));
        header(text(request, "header_name", 80));
        text(request, "value", 4096);
    }

    private void validateExchange(JsonNode request) {
        fields(request, Set.of("type", "token_request", "token_response", "token_placement", "secrets"));
        JsonNode tokenRequest = object(request, "token_request");
        fields(tokenRequest, Set.of("method", "url", "headers", "body_fields"));
        if (!"POST".equals(text(tokenRequest, "method", 8))) {
            throw invalid();
        }
        validateUrl(text(tokenRequest, "url", 2048));
        JsonNode headers = object(tokenRequest, "headers");
        fields(headers, Set.of("Content-Type"));
        String contentType = text(headers, "Content-Type", 80);
        if (!contentType.equals("application/json")
                && !contentType.equals("application/x-www-form-urlencoded")) {
            throw invalid();
        }
        JsonNode body = object(tokenRequest, "body_fields");
        JsonNode secrets = object(request, "secrets");
        if (body.isEmpty() || body.size() > 32 || secrets.isEmpty() || secrets.size() > 32) {
            throw invalid();
        }
        for (Iterator<String> names = secrets.fieldNames(); names.hasNext();) {
            String name = names.next();
            if (!NAME.matcher(name).matches()) {
                throw invalid();
            }
            text(secrets, name, 4096);
        }
        Set<String> referenced = new HashSet<>();
        for (Iterator<String> names = body.fieldNames(); names.hasNext();) {
            String name = names.next();
            if (!NAME.matcher(name).matches()) {
                throw invalid();
            }
            JsonNode field = object(body, name);
            fields(field, Set.of("source", "name", "value"));
            String source = text(field, "source", 20);
            if (source.equals("secret")) {
                if (field.has("value") || !secrets.has(text(field, "name", 80))) {
                    throw invalid();
                }
                referenced.add(field.get("name").asText());
            } else if (source.equals("literal")) {
                if (field.has("name")) {
                    throw invalid();
                }
                text(field, "value", 1024);
            } else {
                throw invalid();
            }
        }
        if (referenced.size() != secrets.size()) {
            throw invalid();
        }
        validateTokenResponse(object(request, "token_response"));
        JsonNode placement = object(request, "token_placement");
        fields(placement, Set.of("type", "name", "prefix"));
        if (!"HEADER".equals(text(placement, "type", 20))
                || !"Authorization".equals(text(placement, "name", 80))
                || !"Bearer ".equals(text(placement, "prefix", 20))) {
            throw invalid();
        }
    }

    private void validateClientCredentials(JsonNode request) {
        fields(request, Set.of("type", "token_url", "client_id", "client_secret", "scopes", "token_response"));
        validateUrl(text(request, "token_url", 2048));
        text(request, "client_id", 512);
        text(request, "client_secret", 4096);
        if (request.has("scopes")) {
            JsonNode scopes = request.get("scopes");
            if (!scopes.isArray() || scopes.size() > 32) {
                throw invalid();
            }
            Set<String> unique = new HashSet<>();
            for (JsonNode scope : scopes) {
                if (!scope.isTextual() || !SCOPE.matcher(scope.asText()).matches()
                        || !unique.add(scope.asText())) {
                    throw invalid();
                }
            }
        }
        validateTokenResponse(object(request, "token_response"));
    }

    private void validateTokenResponse(JsonNode response) {
        fields(response, Set.of("access_token_path", "expires_in_path", "token_type"));
        if (!JSON_PATH.matcher(text(response, "access_token_path", 256)).matches()
                || !"Bearer".equals(text(response, "token_type", 20))) {
            throw invalid();
        }
        if (response.has("expires_in_path")
                && !JSON_PATH.matcher(text(response, "expires_in_path", 256)).matches()) {
            throw invalid();
        }
    }

    private void validateUrl(String url) {
        try {
            URI full = URI.create(url);
            if (full.getScheme() == null || full.getHost() == null || full.getRawUserInfo() != null
                    || full.getRawQuery() != null || full.getRawFragment() != null || full.getRawPath() == null
                    || full.getRawPath().isEmpty()) {
                throw invalid();
            }
            URI base = new URI(full.getScheme(), null, full.getHost(), full.getPort(), null, null, null);
            destinationPolicy.validate(new Destination(base.toString(), full.getRawPath(), Map.of(), Map.of()));
        } catch (URISyntaxException | IllegalArgumentException | InvalidDestinationException malformed) {
            throw invalid();
        }
    }

    private void header(String name) {
        String lower = name.toLowerCase(java.util.Locale.ROOT);
        if (!NAME.matcher(name).matches() || BLOCKED_HEADERS.contains(lower)
                || lower.startsWith("proxy-") || lower.startsWith("x-relayforge-")
                || lower.equals("authorization")) {
            throw invalid();
        }
    }

    private JsonNode object(JsonNode parent, String field) {
        JsonNode result = parent.get(field);
        if (result == null || !result.isObject()) {
            throw invalid();
        }
        return result;
    }

    private String text(JsonNode parent, String field, int maximumLength) {
        JsonNode value = parent.get(field);
        if (value == null || !value.isTextual()) {
            throw invalid();
        }
        String text = value.asText();
        if (text.isBlank() || text.length() > maximumLength
                || text.codePoints().anyMatch(character -> character < 0x20 || character == 0x7f)) {
            throw invalid();
        }
        return text;
    }

    private void fields(JsonNode node, Set<String> allowed) {
        if (!node.isObject()) {
            throw invalid();
        }
        for (Iterator<String> names = node.fieldNames(); names.hasNext();) {
            if (!allowed.contains(names.next())) {
                throw invalid();
            }
        }
    }

    private InvalidAuthenticationException invalid() {
        return new InvalidAuthenticationException();
    }

    public record ValidatedAuthentication(String type, String plaintextJson) {
        @Override
        public String toString() {
            return "ValidatedAuthentication[type=" + type + ", plaintextJson=[REDACTED]]";
        }
    }
}

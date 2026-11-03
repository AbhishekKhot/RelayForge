package com.relayforge.deliveryworker.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.deliveryworker.lease.DeliveryAttemptService;
import com.relayforge.deliveryworker.lease.DeliveryClaimService;
import com.relayforge.outbound.Destination;
import com.relayforge.outbound.DestinationPolicy;
import com.relayforge.outbound.SafeWebhookClient;
import com.relayforge.outbound.UnsafeDestinationException;
import com.relayforge.outbound.ValidatedDestination;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Base64;
import java.util.function.BooleanSupplier;
import java.util.function.DoubleSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;

public final class TokenExchangeService {
    private static final Pattern JSON_PATH = Pattern.compile(
            "\\$((\\.[A-Za-z_][A-Za-z0-9_]*)|(\\[[0-9]+\\]))+");
    private static final Pattern FIELD_NAME = Pattern.compile("[A-Za-z][A-Za-z0-9_-]{0,79}");
    private static final Pattern BEARER_TOKEN = Pattern.compile("[A-Za-z0-9._~+/-]+=*");
    private static final Duration BUDGET = Duration.ofSeconds(90);
    private final SafeWebhookClient client;
    private final DestinationPolicy policy;
    private final ObjectMapper json;
    private final DeliveryAttemptService attempts;
    private final Clock clock;
    private final DoubleSupplier random;
    private final Sleeper sleeper;

    public TokenExchangeService(SafeWebhookClient client, DestinationPolicy policy, ObjectMapper json,
            DeliveryAttemptService attempts, Clock clock, DoubleSupplier random, Sleeper sleeper) {
        this.client = Objects.requireNonNull(client);
        this.policy = Objects.requireNonNull(policy);
        this.json = Objects.requireNonNull(json);
        this.attempts = Objects.requireNonNull(attempts);
        this.clock = Objects.requireNonNull(clock);
        this.random = Objects.requireNonNull(random);
        this.sleeper = Objects.requireNonNull(sleeper);
    }

    public AuthMaterial acquire(JsonNode configuration, DeliveryClaimService.Lease lease,
            BooleanSupplier leaseValid) {
        return acquireToken(configuration, lease, leaseValid).material();
    }

    public AcquiredToken acquireToken(JsonNode configuration, DeliveryClaimService.Lease lease,
            BooleanSupplier leaseValid) {
        Objects.requireNonNull(lease);
        Objects.requireNonNull(leaseValid);
        RequestSpec spec;
        try {
            spec = parse(configuration);
        } catch (RuntimeException invalid) {
            throw new TokenFailure(false, "AUTH_CONFIGURATION_INVALID");
        }
        Instant deadline = clock.instant().plus(BUDGET);
        for (int index = 0; index < 6; index++) {
            if (!leaseValid.getAsBoolean()) {
                throw new TokenFailure(true, "AUTH_LEASE_LOST");
            }
            Duration remaining = Duration.between(clock.instant(), deadline);
            if (remaining.isNegative() || remaining.isZero()) {
                throw new TokenFailure(true, "AUTH_DEADLINE_EXCEEDED");
            }
            Instant startedAt = clock.instant();
            int httpStatus = 0;
            try {
                SafeWebhookClient.TokenResult response = client.sendToken(spec.destination(),
                        spec.body(), spec.contentType(), spec.authorization(),
                        remaining.compareTo(Duration.ofSeconds(10)) < 0
                                ? remaining : Duration.ofSeconds(10), leaseValid);
                httpStatus = response.status();
                if (httpStatus >= 200 && httpStatus < 300) {
                    AcquiredToken token = extract(response, spec);
                    attempts.recordAuthentication(lease, "SUCCEEDED", httpStatus, null, startedAt);
                    return token;
                }
                if (!retryable(httpStatus)) {
                    attempts.recordAuthentication(lease, "TERMINAL_FAILURE", httpStatus,
                            "AUTH_HTTP_TERMINAL", startedAt);
                    throw new TokenFailure(false, "AUTH_HTTP_TERMINAL");
                }
                attempts.recordAuthentication(lease, "RETRYABLE_FAILURE", httpStatus,
                        "AUTH_HTTP_RETRYABLE", startedAt);
            } catch (TokenFailure failure) {
                if (failure.code().equals("AUTH_RESPONSE_INVALID")) {
                    attempts.recordAuthentication(lease, "TERMINAL_FAILURE", httpStatus,
                            failure.code(), startedAt);
                }
                throw failure;
            } catch (UnsafeDestinationException invalid) {
                attempts.recordAuthentication(lease, "TERMINAL_FAILURE", null,
                        "AUTH_DESTINATION_BLOCKED", startedAt);
                throw new TokenFailure(false, "AUTH_DESTINATION_BLOCKED");
            } catch (IOException unavailable) {
                if (tlsFailure(unavailable)) {
                    attempts.recordAuthentication(lease, "TERMINAL_FAILURE", null,
                            "AUTH_TLS_INVALID", startedAt);
                    throw new TokenFailure(false, "AUTH_TLS_INVALID");
                }
                attempts.recordAuthentication(lease, "RETRYABLE_FAILURE", null,
                        "AUTH_NETWORK", startedAt);
            }
            if (index == 5) {
                break;
            }
            double sample = random.getAsDouble();
            if (!Double.isFinite(sample) || sample < 0 || sample >= 1) {
                throw new IllegalStateException("Invalid retry randomness");
            }
            long baseMillis = 1000L << index;
            Duration delay = Duration.ofMillis(baseMillis + (long) (baseMillis * sample));
            if (!clock.instant().plus(delay).isBefore(deadline)) {
                break;
            }
            try {
                sleeper.sleep(delay);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new TokenFailure(true, "AUTH_INTERRUPTED");
            }
        }
        throw new TokenFailure(true, "AUTH_RETRIES_EXHAUSTED");
    }

    private boolean retryable(int status) {
        return status == 408 || status == 429 || status >= 500 && status <= 599;
    }

    private boolean tlsFailure(IOException unavailable) {
        for (Throwable cause = unavailable; cause != null; cause = cause.getCause()) {
            if (cause instanceof SSLHandshakeException || cause instanceof SSLPeerUnverifiedException) {
                return true;
            }
        }
        return false;
    }

    private AcquiredToken extract(SafeWebhookClient.TokenResult response, RequestSpec spec) {
        try {
            if (!response.contentType().toLowerCase(java.util.Locale.ROOT)
                    .startsWith("application/json")) {
                throw new IllegalArgumentException();
            }
            JsonNode body = json.readTree(response.body());
            JsonNode value = path(body, spec.accessPath());
            if (!value.isTextual() || value.asText().length() > 4090
                    || !BEARER_TOKEN.matcher(value.asText()).matches()) {
                throw new IllegalArgumentException();
            }
            Instant expiresAt = null;
            if (spec.expiryPath() != null) {
                JsonNode expiry = path(body, spec.expiryPath());
                if (!expiry.canConvertToLong() || expiry.asLong() <= 0
                        || expiry.asLong() > 86400L * 365) {
                    throw new IllegalArgumentException();
                }
                expiresAt = clock.instant().plusSeconds(expiry.asLong());
            }
            return new AcquiredToken(value.asText(), expiresAt);
        } catch (IOException | RuntimeException invalid) {
            throw new TokenFailure(false, "AUTH_RESPONSE_INVALID");
        }
    }

    private RequestSpec parse(JsonNode config) {
        if (config == null || !config.isObject()) {
            throw new IllegalArgumentException();
        }
        String type = text(config, "type", 40);
        String url;
        String contentType;
        String authorization = null;
        byte[] body;
        if (type.equals("CLIENT_CREDENTIALS")) {
            url = text(config, "token_url", 2048);
            contentType = "application/x-www-form-urlencoded";
            Map<String, String> fields = new LinkedHashMap<>();
            fields.put("grant_type", "client_credentials");
            String clientId = URLEncoder.encode(text(config, "client_id", 512), StandardCharsets.UTF_8);
            String clientSecret = URLEncoder.encode(text(config, "client_secret", 4096),
                    StandardCharsets.UTF_8);
            authorization = "Basic " + Base64.getEncoder().encodeToString((clientId + ":"
                    + clientSecret).getBytes(StandardCharsets.US_ASCII));
            JsonNode scopes = config.path("scopes");
            if (!scopes.isMissingNode()) {
                if (!scopes.isArray() || scopes.size() > 32) {
                    throw new IllegalArgumentException();
                }
                StringBuilder joined = new StringBuilder();
                for (JsonNode scope : scopes) {
                    if (!scope.isTextual() || !scope.asText().matches("[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}")) {
                        throw new IllegalArgumentException();
                    }
                    if (!joined.isEmpty()) {
                        joined.append(' ');
                    }
                    joined.append(scope.asText());
                }
                if (!joined.isEmpty()) {
                    fields.put("scope", joined.toString());
                }
            }
            body = form(fields);
        } else if (type.equals("TOKEN_EXCHANGE")) {
            JsonNode request = object(config, "token_request");
            if (!"POST".equals(text(request, "method", 8))) {
                throw new IllegalArgumentException();
            }
            url = text(request, "url", 2048);
            contentType = text(object(request, "headers"), "Content-Type", 80);
            if (!contentType.equals("application/json")
                    && !contentType.equals("application/x-www-form-urlencoded")) {
                throw new IllegalArgumentException();
            }
            JsonNode fieldsNode = object(request, "body_fields");
            JsonNode secrets = object(config, "secrets");
            if (fieldsNode.isEmpty() || fieldsNode.size() > 32) {
                throw new IllegalArgumentException();
            }
            Map<String, String> fields = new LinkedHashMap<>();
            fieldsNode.fields().forEachRemaining(entry -> {
                if (!FIELD_NAME.matcher(entry.getKey()).matches()) {
                    throw new IllegalArgumentException();
                }
                JsonNode field = entry.getValue();
                String source = text(field, "source", 20);
                String value = source.equals("secret")
                        ? text(secrets, text(field, "name", 80), 4096)
                        : source.equals("literal") ? text(field, "value", 1024) : null;
                if (value == null) {
                    throw new IllegalArgumentException();
                }
                fields.put(entry.getKey(), value);
            });
            try {
                body = contentType.equals("application/json") ? json.writeValueAsBytes(fields) : form(fields);
            } catch (IOException invalid) {
                throw new IllegalArgumentException();
            }
            JsonNode placement = object(config, "token_placement");
            if (!"HEADER".equals(text(placement, "type", 20))
                    || !"Authorization".equals(text(placement, "name", 80))
                    || !"Bearer ".equals(text(placement, "prefix", 20))) {
                throw new IllegalArgumentException();
            }
        } else {
            throw new IllegalArgumentException();
        }
        if (body.length > 16384) {
            throw new IllegalArgumentException();
        }
        JsonNode response = object(config, "token_response");
        String access = text(response, "access_token_path", 256);
        String expiry = response.has("expires_in_path")
                ? text(response, "expires_in_path", 256) : null;
        if (!JSON_PATH.matcher(access).matches() || expiry != null && !JSON_PATH.matcher(expiry).matches()
                || !"Bearer".equals(text(response, "token_type", 20))) {
            throw new IllegalArgumentException();
        }
        URI uri = URI.create(url);
        if (uri.getRawQuery() != null || uri.getRawFragment() != null || uri.getRawUserInfo() != null
                || uri.getRawPath() == null || uri.getRawPath().isEmpty()) {
            throw new IllegalArgumentException();
        }
        URI base = URI.create(uri.getScheme() + "://" + uri.getRawAuthority());
        ValidatedDestination destination = policy.validate(new Destination(base.toString(),
                uri.getRawPath(), Map.of(), Map.of()));
        return new RequestSpec(destination, body, contentType, authorization, access, expiry);
    }

    private byte[] form(Map<String, String> fields) {
        StringBuilder encoded = new StringBuilder();
        fields.forEach((name, value) -> {
            if (!encoded.isEmpty()) {
                encoded.append('&');
            }
            encoded.append(URLEncoder.encode(name, StandardCharsets.UTF_8));
            encoded.append('=');
            encoded.append(URLEncoder.encode(value, StandardCharsets.UTF_8));
        });
        return encoded.toString().getBytes(StandardCharsets.UTF_8);
    }

    private JsonNode path(JsonNode root, String expression) {
        JsonNode value = root;
        Matcher segments = Pattern.compile("\\.([A-Za-z_][A-Za-z0-9_]*)|\\[([0-9]+)\\]")
                .matcher(expression.substring(1));
        while (segments.find()) {
            value = segments.group(1) != null ? value.path(segments.group(1))
                    : value.path(Integer.parseInt(segments.group(2)));
        }
        return value;
    }

    private JsonNode object(JsonNode parent, String field) {
        JsonNode value = parent.path(field);
        if (!value.isObject()) {
            throw new IllegalArgumentException();
        }
        return value;
    }

    private String text(JsonNode parent, String field, int max) {
        JsonNode value = parent.path(field);
        if (!value.isTextual() || value.asText().isBlank() || value.asText().length() > max
                || value.asText().codePoints().anyMatch(character -> character < 0x20
                || character == 0x7f)) {
            throw new IllegalArgumentException();
        }
        return value.asText();
    }

    private record RequestSpec(ValidatedDestination destination, byte[] body, String contentType,
            String authorization, String accessPath, String expiryPath) {
        @Override
        public String toString() {
            return "RequestSpec[destination=[REDACTED], body=[REDACTED], authorization=[REDACTED]]";
        }
    }

    @FunctionalInterface
    public interface Sleeper {
        void sleep(Duration duration) throws InterruptedException;
    }

    public static final class TokenFailure extends RuntimeException {
        private final boolean retryable;
        private final String code;

        public TokenFailure(boolean retryable, String code) {
            super(code);
            this.retryable = retryable;
            this.code = code;
        }

        public boolean retryable() {
            return retryable;
        }

        public String code() {
            return code;
        }
    }

    public record AcquiredToken(String token, Instant expiresAt) {
        public AcquiredToken {
            Objects.requireNonNull(token);
        }

        public AuthMaterial material() {
            return new AuthMaterial(Map.of("Authorization", "Bearer " + token));
        }

        @Override
        public String toString() {
            return "AcquiredToken[token=[REDACTED], expiresAt=" + expiresAt + "]";
        }
    }
}

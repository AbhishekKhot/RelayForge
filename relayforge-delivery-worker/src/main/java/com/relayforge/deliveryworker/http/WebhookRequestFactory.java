package com.relayforge.deliveryworker.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public final class WebhookRequestFactory {
    private final ObjectMapper json;

    public WebhookRequestFactory(ObjectMapper json) {
        this.json = Objects.requireNonNull(json);
    }

    public Request build(UUID deliveryId, String eventLocator, String eventType,
            int attemptNumber, JsonNode data, JsonNode authentication,
            String signingSecret, Instant timestamp) {
        if (deliveryId == null || eventLocator == null || !eventLocator.startsWith("evt_")
                || eventType == null || !eventType.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,199}")
                || attemptNumber < 1 || attemptNumber > 20 || data == null || !data.isObject()
                || authentication == null || !authentication.isObject() || timestamp == null
                || signingSecret == null) {
            throw new IllegalArgumentException("Invalid webhook request data");
        }
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("delivery_id", deliveryId.toString());
        envelope.put("event_id", eventLocator);
        envelope.put("event_type", eventType);
        envelope.put("attempt_number", attemptNumber);
        envelope.put("timestamp", timestamp.toString());
        envelope.put("data", data);
        byte[] body;
        try {
            body = json.writeValueAsBytes(envelope);
        } catch (JsonProcessingException invalid) {
            throw new IllegalStateException("Webhook serialization failed", invalid);
        }
        if (body.length > 300000) {
            throw new IllegalArgumentException("Webhook request is too large");
        }
        String unixSeconds = Long.toString(timestamp.getEpochSecond());
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("X-RelayForge-Event-Id", eventLocator);
        headers.put("X-RelayForge-Delivery-Id", deliveryId.toString());
        headers.put("X-RelayForge-Event-Type", eventType);
        headers.put("X-RelayForge-Timestamp", unixSeconds);
        headers.put("X-RelayForge-Signature", "v1=" + signature(signingSecret, unixSeconds, body));
        applyAuthentication(headers, authentication);
        return new Request(body, headers);
    }

    private String signature(String encodedSecret, String unixSeconds, byte[] body) {
        byte[] key;
        try {
            key = Base64.getUrlDecoder().decode(encodedSecret);
        } catch (IllegalArgumentException malformed) {
            throw new IllegalStateException("Signing secret is unavailable");
        }
        if (key.length != 32) {
            Arrays.fill(key, (byte) 0);
            throw new IllegalStateException("Signing secret is unavailable");
        }
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            mac.update(unixSeconds.getBytes(StandardCharsets.US_ASCII));
            mac.update((byte) '.');
            return HexFormat.of().formatHex(mac.doFinal(body));
        } catch (GeneralSecurityException unavailable) {
            throw new IllegalStateException("Webhook signing failed");
        } finally {
            Arrays.fill(key, (byte) 0);
        }
    }

    private void applyAuthentication(Map<String, String> headers, JsonNode authentication) {
        String type = authentication.path("type").asText("");
        switch (type) {
            case "NONE" -> { }
            case "BEARER" -> headers.put("Authorization", "Bearer " + required(authentication, "token"));
            case "BASIC" -> {
                String credentials = required(authentication, "username") + ":"
                        + required(authentication, "password");
                headers.put("Authorization", "Basic " + Base64.getEncoder().encodeToString(
                        credentials.getBytes(StandardCharsets.UTF_8)));
            }
            case "STATIC_HEADER", "STATIC_API_KEY" -> {
                String name = required(authentication, "header_name");
                if (name.regionMatches(true, 0, "X-RelayForge-", 0, 13)
                        || headers.keySet().stream().anyMatch(existing -> existing.equalsIgnoreCase(name))) {
                    throw new IllegalStateException("Authentication header conflicts with webhook header");
                }
                headers.put(name, required(authentication, "value"));
            }
            default -> throw new IllegalStateException("Authentication strategy is unavailable");
        }
    }

    private String required(JsonNode object, String field) {
        JsonNode value = object.path(field);
        if (!value.isTextual() || value.asText().isBlank()) {
            throw new IllegalStateException("Authentication configuration is unavailable");
        }
        return value.asText();
    }

    public record Request(byte[] body, Map<String, String> headers) {
        public Request {
            body = body.clone();
            headers = Map.copyOf(headers);
        }

        @Override
        public byte[] body() {
            return body.clone();
        }

        @Override
        public String toString() {
            return "Request[body=[REDACTED], headers=[REDACTED]]";
        }
    }
}

package com.relayforge.deliveryworker.http;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.HexFormat;
import java.util.UUID;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;

class WebhookRequestFactoryTest {
    private final ObjectMapper json = new ObjectMapper();
    private final WebhookRequestFactory factory = new WebhookRequestFactory(json);

    @Test
    void signsExactBodyBytesAndKeepsDeliveryIdentityAcrossAttempts() throws Exception {
        UUID delivery = UUID.randomUUID();
        String event = "evt_locator";
        String secret = Base64.getUrlEncoder().withoutPadding()
                .encodeToString("12345678901234567890123456789012".getBytes(StandardCharsets.US_ASCII));
        Instant time = Instant.parse("2026-11-02T10:00:00Z");
        var data = json.readTree("{\"amount\":1700}");
        var auth = json.readTree("{\"type\":\"BEARER\",\"token\":\"private-token\"}");

        WebhookRequestFactory.Request first = factory.build(delivery, event, "payment.completed",
                1, data, auth, secret, time);
        WebhookRequestFactory.Request second = factory.build(delivery, event, "payment.completed",
                2, data, auth, secret, time);

        assertThat(first.headers()).containsEntry("X-RelayForge-Delivery-Id", delivery.toString())
                .containsEntry("X-RelayForge-Event-Id", event)
                .containsEntry("Authorization", "Bearer private-token");
        assertThat(first.headers().get("X-RelayForge-Signature"))
                .isEqualTo("v1=" + hmac(Base64.getUrlDecoder().decode(secret),
                        first.headers().get("X-RelayForge-Timestamp"), first.body()));
        assertThat(new String(first.body(), StandardCharsets.UTF_8))
                .contains("\"delivery_id\":\"" + delivery + "\"")
                .contains("\"attempt_number\":1");
        assertThat(new String(second.body(), StandardCharsets.UTF_8))
                .contains("\"delivery_id\":\"" + delivery + "\"")
                .contains("\"attempt_number\":2");
        assertThat(first.toString()).doesNotContain(secret, "private-token");
    }

    private String hmac(byte[] key, String timestamp, byte[] body) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(key, "HmacSHA256"));
        mac.update(timestamp.getBytes(StandardCharsets.US_ASCII));
        mac.update((byte) '.');
        return HexFormat.of().formatHex(mac.doFinal(body));
    }
}

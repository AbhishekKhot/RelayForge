package com.relayforge.controlplane.endpoint.auth;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EndpointSecretCipherTest {
    private static final String KEY = "0123456789abcdef".repeat(4);

    @Test
    void encryptsWithFreshDataKeysAndContextBinding() {
        var cipher = cipher();
        UUID organization = UUID.randomUUID();
        UUID endpoint = UUID.randomUUID();
        byte[] plaintext = "private-api-token".getBytes(StandardCharsets.UTF_8);

        var first = cipher.encrypt(organization, endpoint, "auth_config", 1, plaintext);
        var second = cipher.encrypt(organization, endpoint, "auth_config", 1, plaintext);

        assertThat(first.ciphertext()).isNotEqualTo(plaintext);
        assertThat(first.ciphertext()).isNotEqualTo(second.ciphertext());
        assertThat(first.wrappedDek()).isNotEqualTo(second.wrappedDek());
        assertThat(first.toString()).doesNotContain("private-api-token", KEY);
        assertThat(new String(cipher.decrypt(organization, endpoint, "auth_config", 1, first),
                StandardCharsets.UTF_8)).isEqualTo("private-api-token");
        assertThatThrownBy(() -> cipher.decrypt(UUID.randomUUID(), endpoint, "auth_config", 1, first))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> cipher.decrypt(organization, endpoint, "signing_secret", 1, first))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> cipher.decrypt(organization, endpoint, "auth_config", 2, first))
                .isInstanceOf(IllegalStateException.class);
        var signing = cipher.encrypt(organization, endpoint, "signing_secret", 1,
                "webhook-signing-secret".getBytes(StandardCharsets.UTF_8));
        assertThat(new String(cipher.decrypt(organization, endpoint, "signing_secret", 1, signing),
                StandardCharsets.UTF_8)).isEqualTo("webhook-signing-secret");
    }

    @Test
    void rejectsMissingWrongAndProductionLocalKeys() {
        assertThatThrownBy(() -> LocalKeyEncryptionService.fromSettings("", "local-v1", true))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> LocalKeyEncryptionService.fromSettings("local-v1:bad", "local-v1", true))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> LocalKeyEncryptionService.fromSettings("local-v1:" + KEY,
                "local-v2", true)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> LocalKeyEncryptionService.fromSettings("local-v1:" + KEY,
                "local-v1", false)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void retainsOldWrappingKeysDuringRotation() {
        var before = new EndpointSecretCipher(LocalKeyEncryptionService.fromSettings(
                "local-v1:" + KEY, "local-v1", true), new SecureRandom());
        UUID organization = UUID.randomUUID();
        UUID endpoint = UUID.randomUUID();
        var secret = before.encrypt(organization, endpoint, "auth_config", 1,
                "value".getBytes(StandardCharsets.UTF_8));
        String nextKey = "fedcba9876543210".repeat(4);
        var after = new EndpointSecretCipher(LocalKeyEncryptionService.fromSettings(
                "local-v1:" + KEY + ",local-v2:" + nextKey, "local-v2", true), new SecureRandom());

        assertThat(new String(after.decrypt(organization, endpoint, "auth_config", 1, secret),
                StandardCharsets.UTF_8)).isEqualTo("value");
        var rewrapped = after.rewrap(organization, endpoint, "auth_config", 1, secret);
        assertThat(rewrapped.keyVersion()).isEqualTo("local-v2");
        assertThat(new String(after.decrypt(organization, endpoint, "auth_config", 1, rewrapped),
                StandardCharsets.UTF_8)).isEqualTo("value");
        assertThatThrownBy(() -> before.decrypt(organization, endpoint, "auth_config", 1, rewrapped))
                .isInstanceOf(IllegalStateException.class);
    }

    private EndpointSecretCipher cipher() {
        return new EndpointSecretCipher(LocalKeyEncryptionService.fromSettings(
                "local-v1:" + KEY, "local-v1", true), new SecureRandom());
    }
}

package com.relayforge.controlplane;

import com.relayforge.controlplane.security.ApiKeyCodec;
import java.security.SecureRandom;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ApiKeyCodecTest {
    private final ApiKeyCodec codec = new ApiKeyCodec(new SecureRandom(), new byte[32]);

    @Test
    void issuesHighEntropyKeysAndVerifiesWithoutStoringPlaintext() {
        var first = codec.issue();
        var second = codec.issue();

        assertThat(first.plaintext()).startsWith("rf_live_" + first.keyId() + "_");
        assertThat(first.plaintext()).isNotEqualTo(second.plaintext());
        assertThat(first.digest()).hasSize(32);
        assertThat(first.toString()).doesNotContain(first.plaintext());
        assertThat(codec.verify(first.plaintext(), first.digest())).isTrue();
        assertThat(codec.verify(first.plaintext() + "x", first.digest())).isFalse();
        assertThat(codec.verify(second.plaintext(), first.digest())).isFalse();
    }

    @Test
    void rejectsMalformedKeys() {
        assertThat(codec.keyId("rf_live_bad")).isEmpty();
        assertThat(codec.keyId("rf_live_zz_notbase64")).isEmpty();
        assertThat(codec.verify("not-a-key", new byte[32])).isFalse();
    }
}

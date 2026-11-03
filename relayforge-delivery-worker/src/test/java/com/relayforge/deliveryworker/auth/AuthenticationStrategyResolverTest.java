package com.relayforge.deliveryworker.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AuthenticationStrategyResolverTest {
    private final ObjectMapper json = new ObjectMapper();
    private final AuthenticationStrategyResolver resolver = new AuthenticationStrategyResolver(
            new StaticAuthenticationStrategy(), (configuration, lease, leaseValid) ->
                    new AuthMaterial(Map.of("Authorization", "Bearer dynamic")));

    @Test
    void resolvesStaticAndDynamicStrategiesWithoutExposingMaterial() throws Exception {
        assertThat(resolver.resolve(json.readTree("{\"type\":\"NONE\"}"), null, () -> true).headers())
                .isEmpty();
        assertThat(resolver.resolve(json.readTree("""
                {"type":"BASIC","username":"user","password":"secret"}
                """), null, () -> true).headers())
                .containsEntry("Authorization", "Basic dXNlcjpzZWNyZXQ=");
        assertThat(resolver.resolve(json.readTree("""
                {"type":"STATIC_API_KEY","header_name":"X-Api-Key","value":"private"}
                """), null, () -> true).headers()).containsEntry("X-Api-Key", "private");
        var dynamic = json.readTree("{\"type\":\"TOKEN_EXCHANGE\"}");
        assertThat(resolver.dynamic(dynamic)).isTrue();
        assertThat(resolver.resolve(dynamic, null, () -> true).headers())
                .containsEntry("Authorization", "Bearer dynamic");
        assertThat(new AuthMaterial(Map.of("Authorization", "Bearer private")).toString())
                .doesNotContain("private");
        assertThatThrownBy(() -> resolver.resolve(json.readTree("{\"type\":\"UNKNOWN\"}"),
                null, () -> true)).isInstanceOf(IllegalArgumentException.class);
    }
}

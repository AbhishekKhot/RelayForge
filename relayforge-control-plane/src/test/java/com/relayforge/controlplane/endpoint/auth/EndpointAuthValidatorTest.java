package com.relayforge.controlplane.endpoint.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.outbound.DestinationPolicy;
import java.net.InetAddress;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EndpointAuthValidatorTest {
    private final ObjectMapper json = new ObjectMapper();
    private final EndpointAuthValidator validator = new EndpointAuthValidator(
            new DestinationPolicy(host -> new InetAddress[]{InetAddress.getByName("8.8.8.8")}, false, null));

    @Test
    void acceptsAllowlistedStaticTypesWithoutLeakingSecretsInToString() throws Exception {
        for (String body : new String[]{
                "{\"type\":\"NONE\"}",
                "{\"type\":\"STATIC_HEADER\",\"header_name\":\"X-Api-Key\",\"value\":\"static-secret\"}",
                "{\"type\":\"STATIC_API_KEY\",\"header_name\":\"X-Api-Key\",\"value\":\"static-secret\"}",
                "{\"type\":\"BEARER\",\"token\":\"bearer-secret\"}",
                "{\"type\":\"BASIC\",\"username\":\"customer\",\"password\":\"password-secret\"}"}) {
            var validated = validator.validate(json.readTree(body));
            assertThat(validated.type()).isNotBlank();
            assertThat(validated.toString()).doesNotContain("secret", "password");
        }
    }

    @Test
    void validatesDeclarativeTokenExchangeAndClientCredentials() throws Exception {
        var exchange = validator.validate(json.readTree("""
                {"type":"TOKEN_EXCHANGE",
                 "token_request":{"method":"POST","url":"https://auth.example.test/token",
                   "headers":{"Content-Type":"application/json"},
                   "body_fields":{"username":{"source":"secret","name":"username"}}},
                 "token_response":{"access_token_path":"$.data.accessToken","expires_in_path":"$.data.expiresIn",
                   "token_type":"Bearer"},
                 "token_placement":{"type":"HEADER","name":"Authorization","prefix":"Bearer "},
                 "secrets":{"username":"customer-secret"}}
                """));
        assertThat(exchange.type()).isEqualTo("TOKEN_EXCHANGE");
        assertThat(exchange.toString()).doesNotContain("customer-secret");

        var oauth = validator.validate(json.readTree("""
                {"type":"CLIENT_CREDENTIALS", "token_url":"https://auth.example.test/oauth/token",
                 "client_id":"client", "client_secret":"oauth-secret", "scopes":["payments.write"],
                 "token_response":{"access_token_path":"$.access_token","expires_in_path":"$.expires_in",
                   "token_type":"Bearer"}}
                """));
        assertThat(oauth.type()).isEqualTo("CLIENT_CREDENTIALS");
        assertThat(oauth.toString()).doesNotContain("oauth-secret");
    }

    @Test
    void rejectsUnsafeDynamicConfigAndUnknownFields() throws Exception {
        for (String body : new String[]{
                "{\"type\":\"NONE\",\"secret\":\"ignored\"}",
                "{\"type\":\"BEARER\",\"token\":\"\"}",
                "{\"type\":\"BASIC\",\"username\":\"x:y\",\"password\":\"secret\"}",
                "{\"type\":\"STATIC_HEADER\",\"header_name\":\"Host\",\"value\":\"internal\"}",
                "{\"type\":\"TOKEN_EXCHANGE\",\"script\":\"return env\"}",
                "{\"type\":\"CLIENT_CREDENTIALS\",\"token_url\":\"http://169.254.169.254/token\",\"client_id\":\"x\",\"client_secret\":\"y\"}",
                "{\"type\":\"CLIENT_CREDENTIALS\",\"token_url\":\"https://auth.example.test/token\",\"client_id\":\"x\",\"client_secret\":\"y\",\"token_response\":{\"access_token_path\":\"$..token\",\"token_type\":\"Bearer\"}}",
                "{\"type\":\"TOKEN_EXCHANGE\",\"token_request\":{\"method\":\"GET\",\"url\":\"https://auth.example.test/token\"},\"token_response\":{\"access_token_path\":\"$.token\",\"token_type\":\"Bearer\"},\"token_placement\":{\"type\":\"HEADER\",\"name\":\"Authorization\",\"prefix\":\"Bearer \"},\"secrets\":{}}"}) {
            JsonNode request = json.readTree(body);
            assertThatThrownBy(() -> validator.validate(request))
                    .isInstanceOf(InvalidAuthenticationException.class);
        }
    }
}

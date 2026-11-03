package com.relayforge.deliveryworker.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.deliveryworker.lease.DeliveryAttemptService;
import com.relayforge.deliveryworker.lease.DeliveryClaimService;
import com.relayforge.outbound.DestinationPolicy;
import com.relayforge.outbound.SafeWebhookClient;
import java.net.InetAddress;
import java.net.URI;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import javax.net.ssl.SSLHandshakeException;

class TokenExchangeServiceTest {
    private static final Instant NOW = Instant.parse("2026-11-03T10:00:00Z");
    private final ObjectMapper json = new ObjectMapper();
    private final SafeWebhookClient client = mock(SafeWebhookClient.class);
    private final DeliveryAttemptService attempts = mock(DeliveryAttemptService.class);
    private final DestinationPolicy policy = new DestinationPolicy(host -> new InetAddress[]{
            InetAddress.getByName("127.0.0.1")}, true, URI.create("http://localhost:18080"));
    private final TokenExchangeService service = new TokenExchangeService(client, policy, json,
            attempts, Clock.fixed(NOW, ZoneOffset.UTC), () -> 0.5, duration -> { });
    private final DeliveryClaimService.Lease lease = new DeliveryClaimService.Lease(UUID.randomUUID(),
            LocalDate.parse("2026-11-03"), UUID.randomUUID(), UUID.randomUUID(), "worker",
            UUID.randomUUID(), 1, NOW, NOW.plusSeconds(60), NOW.plusSeconds(240));

    @Test
    void clientCredentialsUsesFormBodyAndExtractsNestedToken() throws Exception {
        when(client.sendToken(any(), any(), eq("application/x-www-form-urlencoded"),
                any(String.class), any(Duration.class), any())).thenReturn(new SafeWebhookClient.TokenResult(200,
                "{\"data\":{\"accessToken\":\"safe-token\",\"expiresIn\":120}}"
                        .getBytes(StandardCharsets.UTF_8), "application/json"));
        var configuration = json.readTree("""
                {"type":"CLIENT_CREDENTIALS","token_url":"http://localhost:18080/token",
                 "client_id":"client","client_secret":"secret","scopes":["orders:write"],
                 "token_response":{"access_token_path":"$.data.accessToken",
                 "expires_in_path":"$.data.expiresIn","token_type":"Bearer"}}
                """);
        var acquired = service.acquireToken(configuration, lease, () -> true);
        assertThat(acquired.material().headers()).containsEntry("Authorization", "Bearer safe-token");
        assertThat(acquired.expiresAt()).isEqualTo(NOW.plusSeconds(120));
        verify(client).sendToken(any(), org.mockito.ArgumentMatchers.argThat(body ->
                new String(body, StandardCharsets.UTF_8).contains("grant_type=client_credentials")
                && !new String(body, StandardCharsets.UTF_8).contains("client_secret=secret")),
                eq("application/x-www-form-urlencoded"),
                eq("Basic " + java.util.Base64.getEncoder().encodeToString(
                        "client:secret".getBytes(StandardCharsets.US_ASCII))),
                any(Duration.class), any());
        verify(attempts).recordAuthentication(eq(lease), eq("SUCCEEDED"), eq(200),
                eq(null), any());
    }

    @Test
    void retriesTransientStatusThenSucceedsWithoutWebhookAttempt() throws Exception {
        when(client.sendToken(any(), any(), eq("application/x-www-form-urlencoded"),
                any(String.class), any(Duration.class), any())).thenReturn(
                new SafeWebhookClient.TokenResult(500, new byte[0], "application/json"),
                new SafeWebhookClient.TokenResult(429, new byte[0], "application/json"),
                new SafeWebhookClient.TokenResult(200,
                        "{\"access_token\":\"new-token\"}".getBytes(StandardCharsets.UTF_8),
                        "application/json"));
        assertThat(service.acquire(credentials(), lease, () -> true).headers())
                .containsEntry("Authorization", "Bearer new-token");
        verify(attempts, atLeastOnce()).recordAuthentication(eq(lease), eq("RETRYABLE_FAILURE"),
                any(), any(), any());
    }

    @Test
    void malformedTokenAndInvalidConfigurationFailFast() throws Exception {
        when(client.sendToken(any(), any(), eq("application/x-www-form-urlencoded"),
                any(String.class), any(Duration.class), any())).thenReturn(new SafeWebhookClient.TokenResult(200,
                "{\"access_token\":null}".getBytes(StandardCharsets.UTF_8), "application/json"));
        assertThatThrownBy(() -> service.acquire(credentials(), lease, () -> true))
                .isInstanceOfSatisfying(TokenExchangeService.TokenFailure.class,
                        failure -> assertThat(failure.retryable()).isFalse());
        assertThatThrownBy(() -> service.acquire(json.readTree("""
                {"type":"CLIENT_CREDENTIALS","token_url":"http://169.254.169.254/token"}
                """), lease, () -> true))
                .isInstanceOfSatisfying(TokenExchangeService.TokenFailure.class,
                        failure -> assertThat(failure.retryable()).isFalse());
    }

    @Test
    void stopsAfterInitialAttemptAndFiveTransientRetries() throws Exception {
        when(client.sendToken(any(), any(), eq("application/x-www-form-urlencoded"),
                any(String.class), any(Duration.class), any()))
                .thenReturn(new SafeWebhookClient.TokenResult(500, new byte[0], "application/json"));
        assertThatThrownBy(() -> service.acquire(credentials(), lease, () -> true))
                .isInstanceOfSatisfying(TokenExchangeService.TokenFailure.class,
                        failure -> assertThat(failure.retryable()).isTrue());
        verify(client, times(6)).sendToken(any(), any(), eq("application/x-www-form-urlencoded"),
                any(String.class), any(Duration.class), any());
        verify(attempts, times(6)).recordAuthentication(eq(lease), eq("RETRYABLE_FAILURE"),
                eq(500), eq("AUTH_HTTP_RETRYABLE"), any());
    }

    @Test
    void rejectsCertificateFailureWithoutRetry() throws Exception {
        when(client.sendToken(any(), any(), eq("application/x-www-form-urlencoded"),
                any(String.class), any(Duration.class), any()))
                .thenThrow(new IOException("transport", new SSLHandshakeException("invalid")));
        assertThatThrownBy(() -> service.acquire(credentials(), lease, () -> true))
                .isInstanceOfSatisfying(TokenExchangeService.TokenFailure.class,
                        failure -> assertThat(failure.retryable()).isFalse());
        verify(client, times(1)).sendToken(any(), any(), eq("application/x-www-form-urlencoded"),
                any(String.class), any(Duration.class), any());
        verify(attempts).recordAuthentication(eq(lease), eq("TERMINAL_FAILURE"),
                eq(null), eq("AUTH_TLS_INVALID"), any());
    }

    private com.fasterxml.jackson.databind.JsonNode credentials() throws Exception {
        return json.readTree("""
                {"type":"CLIENT_CREDENTIALS","token_url":"http://localhost:18080/token",
                 "client_id":"client","client_secret":"secret",
                 "token_response":{"access_token_path":"$.access_token","token_type":"Bearer"}}
                """);
    }
}

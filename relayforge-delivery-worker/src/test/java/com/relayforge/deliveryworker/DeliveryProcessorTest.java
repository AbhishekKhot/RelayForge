package com.relayforge.deliveryworker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.deliveryworker.http.DeliveryReferenceClient;
import com.relayforge.deliveryworker.auth.AuthenticationStrategyResolver;
import com.relayforge.deliveryworker.auth.StaticAuthenticationStrategy;
import com.relayforge.deliveryworker.http.WebhookRequestFactory;
import com.relayforge.deliveryworker.lease.DeliveryAttemptService;
import com.relayforge.deliveryworker.lease.DeliveryClaimService;
import com.relayforge.deliveryworker.retry.RetryPolicy;
import com.relayforge.outbound.DestinationPolicy;
import com.relayforge.outbound.SafeWebhookClient;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.Optional;
import java.util.UUID;
import javax.net.ssl.SSLHandshakeException;
import org.junit.jupiter.api.Test;

class DeliveryProcessorTest {
    @Test
    void treatsWrappedTlsCertificateFailureAsTerminal() {
        assertThat(DeliveryProcessor.failureFor(new IOException("transport",
                new SSLHandshakeException("certificate rejected"))))
                .isEqualTo(RetryPolicy.Failure.TLS);
    }

    @Test
    void rejectsInvalidEndpointPolicyBeforeClaimingOrSending() throws Exception {
        UUID deliveryId = UUID.randomUUID();
        UUID organizationId = UUID.randomUUID();
        UUID endpointId = UUID.randomUUID();
        Instant now = Instant.parse("2026-11-02T10:00:00Z");
        DeliveryClaimService claims = mock(DeliveryClaimService.class);
        DeliveryAttemptService attempts = mock(DeliveryAttemptService.class);
        DeliveryReferenceClient references = mock(DeliveryReferenceClient.class);
        SafeWebhookClient client = mock(SafeWebhookClient.class);
        when(attempts.candidate(deliveryId)).thenReturn(new DeliveryAttemptService.WorkReference(
                organizationId, endpointId, 0, now.minusSeconds(60), now.plusSeconds(3600)));
        when(references.endpoint(organizationId, endpointId)).thenReturn(new ObjectMapper().readTree("""
                {"max_concurrency":0,"max_attempts":3,"version":1}
                """));
        DestinationPolicy policy = new DestinationPolicy(host -> new InetAddress[]{
                InetAddress.getByName("127.0.0.1")}, true, URI.create("http://localhost:18080"));
        try (DeliveryProcessor processor = new DeliveryProcessor(claims, attempts, references,
                new WebhookRequestFactory(new ObjectMapper()), policy, client,
                new RetryPolicy(Clock.fixed(now, ZoneOffset.UTC), () -> 0.5,
                        new RetryPolicy.Settings(Duration.ofSeconds(1), Duration.ofMinutes(15),
                                Duration.ofHours(1), Duration.ofHours(24))),
                Clock.fixed(now, ZoneOffset.UTC), "worker-test")) {
            processor.process(deliveryId);
        }
        verify(attempts).rejectUnclaimed(deliveryId, "CONFIGURATION_INVALID");
        verifyNoInteractions(claims, client);
    }

    @Test
    void pausesNewClaimsWhenRedisQuotaCoordinationIsUnavailable() throws Exception {
        UUID deliveryId = UUID.randomUUID();
        UUID organizationId = UUID.randomUUID();
        UUID endpointId = UUID.randomUUID();
        Instant now = Instant.parse("2026-11-02T10:00:00Z");
        DeliveryClaimService claims = mock(DeliveryClaimService.class);
        DeliveryAttemptService attempts = mock(DeliveryAttemptService.class);
        DeliveryReferenceClient references = mock(DeliveryReferenceClient.class);
        SafeWebhookClient client = mock(SafeWebhookClient.class);
        when(attempts.candidate(deliveryId)).thenReturn(new DeliveryAttemptService.WorkReference(
                organizationId, endpointId, 0, now.minusSeconds(60), now.plusSeconds(3600)));
        when(references.endpoint(organizationId, endpointId)).thenReturn(new ObjectMapper().readTree("""
                {"max_concurrency":2,"organization_concurrency_limit":1,
                 "max_attempts":3,"version":1}
                """));
        DestinationPolicy policy = new DestinationPolicy(host -> new InetAddress[]{
                InetAddress.getByName("127.0.0.1")}, true, URI.create("http://localhost:18080"));
        var authentication = new AuthenticationStrategyResolver(new StaticAuthenticationStrategy(),
                (configuration, lease, leaseValid) -> {
                    throw new IllegalStateException("Unused token strategy");
                });
        try (DeliveryProcessor processor = new DeliveryProcessor(claims, attempts, references,
                new WebhookRequestFactory(new ObjectMapper()), policy, client,
                new RetryPolicy(Clock.fixed(now, ZoneOffset.UTC), () -> 0.5,
                        new RetryPolicy.Settings(Duration.ofSeconds(1), Duration.ofMinutes(15),
                                Duration.ofHours(1), Duration.ofHours(24))),
                authentication, () -> false, Clock.fixed(now, ZoneOffset.UTC), "worker-test")) {
            processor.process(deliveryId);
        }
        verify(attempts).deferUnclaimed(deliveryId, now.plusSeconds(30));
        verifyNoInteractions(claims, client);
    }

    @Test
    void cancelsWorkWhenEndpointIsDisabledBeforeClaim() throws Exception {
        UUID deliveryId = UUID.randomUUID();
        UUID organizationId = UUID.randomUUID();
        UUID endpointId = UUID.randomUUID();
        Instant now = Instant.parse("2026-11-02T10:00:00Z");
        DeliveryClaimService claims = mock(DeliveryClaimService.class);
        DeliveryAttemptService attempts = mock(DeliveryAttemptService.class);
        DeliveryReferenceClient references = mock(DeliveryReferenceClient.class);
        SafeWebhookClient client = mock(SafeWebhookClient.class);
        when(attempts.candidate(deliveryId)).thenReturn(new DeliveryAttemptService.WorkReference(
                organizationId, endpointId, 0, now.minusSeconds(60), now.plusSeconds(3600)));
        when(references.endpoint(organizationId, endpointId)).thenThrow(
                new DeliveryReferenceClient.EndpointNotActiveException());
        DestinationPolicy policy = new DestinationPolicy(host -> new InetAddress[]{
                InetAddress.getByName("127.0.0.1")}, true, URI.create("http://localhost:18080"));
        try (DeliveryProcessor processor = new DeliveryProcessor(claims, attempts, references,
                new WebhookRequestFactory(new ObjectMapper()), policy, client,
                new RetryPolicy(Clock.fixed(now, ZoneOffset.UTC), () -> 0.5,
                        new RetryPolicy.Settings(Duration.ofSeconds(1), Duration.ofMinutes(15),
                                Duration.ofHours(1), Duration.ofHours(24))),
                Clock.fixed(now, ZoneOffset.UTC), "worker-test")) {
            processor.process(deliveryId);
        }
        verify(attempts).cancelUnclaimed(deliveryId);
        verifyNoInteractions(claims, client);
    }

    @Test
    void defersChangedConfigurationBeforeReservingWebhookAttempt() throws Exception {
        UUID deliveryId = UUID.randomUUID();
        UUID organizationId = UUID.randomUUID();
        UUID endpointId = UUID.randomUUID();
        Instant now = Instant.parse("2026-11-02T10:00:00Z");
        DeliveryClaimService.Lease lease = new DeliveryClaimService.Lease(deliveryId,
                LocalDate.parse("2026-11-02"), organizationId, endpointId, "worker-test",
                UUID.randomUUID(), 2, now, now.plusSeconds(60), now.plusSeconds(240));
        DeliveryClaimService claims = mock(DeliveryClaimService.class);
        DeliveryAttemptService attempts = mock(DeliveryAttemptService.class);
        DeliveryReferenceClient references = mock(DeliveryReferenceClient.class);
        SafeWebhookClient client = mock(SafeWebhookClient.class);
        ObjectMapper json = new ObjectMapper();
        String secret = Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[32]);
        String configuration = """
                {"organization_id":"%s","endpoint_id":"%s","base_url":"http://localhost:18080",
                 "path":"/hook","query_params":{},"headers":{},"version":%d,"timeout_ms":1000,
                 "max_attempts":3,"max_concurrency":2,"organization_concurrency_limit":100,
                 "authentication":{"type":"NONE"},"signing_secret":"%s"}
                """;
        when(attempts.candidate(deliveryId)).thenReturn(new DeliveryAttemptService.WorkReference(
                organizationId, endpointId, 0, now.minusSeconds(60), now.plusSeconds(3600)));
        when(claims.claim(deliveryId, "worker-test", 2, 100, 200)).thenReturn(Optional.of(lease));
        when(attempts.eventLocator(lease)).thenReturn("evt_test");
        when(references.endpoint(organizationId, endpointId)).thenReturn(
                json.readTree(configuration.formatted(organizationId, endpointId, 1, secret)),
                json.readTree(configuration.formatted(organizationId, endpointId, 1, secret)),
                json.readTree(configuration.formatted(organizationId, endpointId, 2, secret)));
        when(references.payload(organizationId, "evt_test")).thenReturn(json.readTree("""
                {"organization_id":"%s","locator":"evt_test","event_type":"test.event",
                 "payload":{"key":"value"}}
                """.formatted(organizationId)));
        DestinationPolicy policy = new DestinationPolicy(host -> new InetAddress[]{
                InetAddress.getByName("127.0.0.1")}, true, URI.create("http://localhost:18080"));
        try (DeliveryProcessor processor = new DeliveryProcessor(claims, attempts, references,
                new WebhookRequestFactory(json), policy, client,
                new RetryPolicy(Clock.fixed(now, ZoneOffset.UTC), () -> 0.5,
                        new RetryPolicy.Settings(Duration.ofSeconds(1), Duration.ofMinutes(15),
                                Duration.ofHours(1), Duration.ofHours(24))),
                Clock.fixed(now, ZoneOffset.UTC), "worker-test")) {
            processor.process(deliveryId);
        }
        verify(attempts).deferClaimed(lease, now.plusSeconds(30), "CONFIGURATION_CHANGED");
        verify(attempts, org.mockito.Mockito.never()).reserve(any(),
                org.mockito.ArgumentMatchers.anyInt());
        verifyNoInteractions(client);
    }

    @Test
    void defersEligibleWorkWhenConcurrencyCapRejectsClaim() throws Exception {
        UUID deliveryId = UUID.randomUUID();
        UUID organizationId = UUID.randomUUID();
        UUID endpointId = UUID.randomUUID();
        Instant now = Instant.parse("2026-11-02T10:00:00Z");
        DeliveryClaimService claims = mock(DeliveryClaimService.class);
        DeliveryAttemptService attempts = mock(DeliveryAttemptService.class);
        DeliveryReferenceClient references = mock(DeliveryReferenceClient.class);
        SafeWebhookClient client = mock(SafeWebhookClient.class);
        when(attempts.candidate(deliveryId)).thenReturn(new DeliveryAttemptService.WorkReference(
                organizationId, endpointId, 0, now.minusSeconds(60), now.plusSeconds(3600)));
        when(references.endpoint(organizationId, endpointId)).thenReturn(new ObjectMapper().readTree("""
                {"max_concurrency":2,"organization_concurrency_limit":10,
                 "max_attempts":3,"version":1}
                """));
        DestinationPolicy policy = new DestinationPolicy(host -> new InetAddress[]{
                InetAddress.getByName("127.0.0.1")}, true, URI.create("http://localhost:18080"));
        try (DeliveryProcessor processor = new DeliveryProcessor(claims, attempts, references,
                new WebhookRequestFactory(new ObjectMapper()), policy, client,
                new RetryPolicy(Clock.fixed(now, ZoneOffset.UTC), () -> 0.5,
                        new RetryPolicy.Settings(Duration.ofSeconds(1), Duration.ofMinutes(15),
                                Duration.ofHours(1), Duration.ofHours(24))),
                new AuthenticationStrategyResolver(new StaticAuthenticationStrategy(),
                        (configuration, lease, leaseValid) -> {
                            throw new IllegalStateException("Unused token strategy");
                        }), () -> true, Clock.fixed(now, ZoneOffset.UTC), "worker-test",
                (outcome, elapsed) -> { }, 4)) {
            processor.process(deliveryId);
        }
        verify(claims).claim(deliveryId, "worker-test", 2, 10, 4);
        verify(attempts).deferUnclaimed(deliveryId, now.plusSeconds(30));
        verifyNoInteractions(client);
    }

    @Test
    void usesRetryAfterFor429AndTerminates400() throws Exception {
        assertOutcome(429, "120", Instant.parse("2026-11-02T10:02:00Z"),
                "HTTP_RETRYABLE");
        assertOutcome(400, null, null, "HTTP_TERMINAL");
    }

    private void assertOutcome(int status, String retryAfter, Instant nextAttemptAt,
            String errorCode) throws Exception {
        UUID deliveryId = UUID.randomUUID();
        UUID organizationId = UUID.randomUUID();
        UUID endpointId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        Instant now = Instant.parse("2026-11-02T10:00:00Z");
        DeliveryClaimService.Lease lease = new DeliveryClaimService.Lease(deliveryId,
                LocalDate.parse("2026-11-02"), organizationId, endpointId, "worker-test",
                UUID.randomUUID(), 2, now, now.plusSeconds(60), now.plusSeconds(240));
        DeliveryClaimService claims = mock(DeliveryClaimService.class);
        DeliveryAttemptService attempts = mock(DeliveryAttemptService.class);
        DeliveryReferenceClient references = mock(DeliveryReferenceClient.class);
        SafeWebhookClient client = mock(SafeWebhookClient.class);
        ObjectMapper json = new ObjectMapper();
        String secret = Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[32]);
        URI simulator = URI.create("http://localhost:18080");
        DestinationPolicy policy = new DestinationPolicy(
                host -> new InetAddress[]{InetAddress.getByName("127.0.0.1")}, true, simulator);
        when(attempts.candidate(deliveryId)).thenReturn(new DeliveryAttemptService.WorkReference(
                organizationId, endpointId, 0, now.minusSeconds(60), now.plusSeconds(3600)));
        when(claims.claim(deliveryId, "worker-test", 2, 100, 200)).thenReturn(Optional.of(lease));
        when(claims.owns(lease)).thenReturn(true);
        when(attempts.eventLocator(lease)).thenReturn("evt_test");
        when(attempts.reserve(lease, 3)).thenReturn(new DeliveryAttemptService.Attempt(
                UUID.randomUUID(), eventId, "evt_test", 1));
        when(references.endpoint(organizationId, endpointId)).thenReturn(json.readTree("""
                {"organization_id":"%s","endpoint_id":"%s","base_url":"http://localhost:18080",
                 "path":"/hook","query_params":{},"headers":{},"version":1,"timeout_ms":1000,
                 "max_attempts":3,"max_concurrency":2,"organization_concurrency_limit":100,
                 "authentication":{"type":"NONE"},
                 "signing_secret":"%s"}
                """.formatted(organizationId, endpointId, secret)));
        when(references.payload(organizationId, "evt_test")).thenReturn(json.readTree("""
                {"organization_id":"%s","locator":"evt_test","event_type":"order.created",
                 "payload":{"order_id":"123"}}
                """.formatted(organizationId)));
        when(client.send(any(), any(), eq(Duration.ofSeconds(1)), any(), any()))
                .thenReturn(new SafeWebhookClient.Result(status, null, retryAfter));

        try (DeliveryProcessor processor = new DeliveryProcessor(claims, attempts, references,
                new WebhookRequestFactory(json), policy, client,
                new RetryPolicy(Clock.fixed(now, ZoneOffset.UTC), () -> 0.5,
                        new RetryPolicy.Settings(Duration.ofSeconds(1), Duration.ofMinutes(15),
                                Duration.ofHours(1), Duration.ofHours(24))),
                Clock.fixed(now, ZoneOffset.UTC), "worker-test")) {
            processor.process(deliveryId);
        }
        verify(attempts).finish(eq(lease), any(), eq(status), eq(errorCode),
                eq(null), eq(nextAttemptAt));
    }

    @Test
    void sendsSignedWebhookAndFinalizesSecondAttempt() throws Exception {
        UUID deliveryId = UUID.randomUUID();
        UUID organizationId = UUID.randomUUID();
        UUID endpointId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        Instant now = Instant.parse("2026-11-02T10:00:00Z");
        DeliveryClaimService.Lease lease = new DeliveryClaimService.Lease(deliveryId,
                LocalDate.parse("2026-11-02"), organizationId, endpointId, "worker-test",
                UUID.randomUUID(), 2, now, now.plusSeconds(60), now.plusSeconds(240));
        DeliveryClaimService claims = mock(DeliveryClaimService.class);
        DeliveryAttemptService attempts = mock(DeliveryAttemptService.class);
        DeliveryReferenceClient references = mock(DeliveryReferenceClient.class);
        SafeWebhookClient client = mock(SafeWebhookClient.class);
        ObjectMapper json = new ObjectMapper();
        String secret = Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[32]);
        URI simulator = URI.create("http://localhost:18080");
        DestinationPolicy policy = new DestinationPolicy(
                host -> new InetAddress[]{InetAddress.getByName("127.0.0.1")}, true, simulator);
        when(attempts.candidate(deliveryId)).thenReturn(new DeliveryAttemptService.WorkReference(
                organizationId, endpointId, 1, now.minusSeconds(60), now.plusSeconds(3600)));
        when(claims.claim(deliveryId, "worker-test", 2, 100, 200)).thenReturn(Optional.of(lease));
        when(claims.owns(lease)).thenReturn(true);
        when(attempts.eventLocator(lease)).thenReturn("evt_test");
        when(attempts.reserve(lease, 3)).thenReturn(new DeliveryAttemptService.Attempt(
                UUID.randomUUID(), eventId, "evt_test", 2));
        when(references.endpoint(organizationId, endpointId)).thenReturn(json.readTree("""
                {"organization_id":"%s","endpoint_id":"%s","base_url":"http://localhost:18080",
                 "path":"/hook","query_params":{},"headers":{},"version":1,"timeout_ms":1000,
                 "max_attempts":3,"max_concurrency":2,"organization_concurrency_limit":100,
                 "authentication":{"type":"NONE"},
                 "signing_secret":"%s"}
                """.formatted(organizationId, endpointId, secret)));
        when(references.payload(organizationId, "evt_test")).thenReturn(json.readTree("""
                {"organization_id":"%s","locator":"evt_test","event_type":"order.created",
                 "payload":{"order_id":"123"}}
                """.formatted(organizationId)));
        when(client.send(any(), any(), eq(Duration.ofSeconds(1)), any(), any()))
                .thenReturn(new SafeWebhookClient.Result(202, "received " + secret));

        try (DeliveryProcessor processor = new DeliveryProcessor(claims, attempts, references,
                new WebhookRequestFactory(json), policy, client,
                new RetryPolicy(Clock.fixed(now, ZoneOffset.UTC), () -> 0.5,
                        new RetryPolicy.Settings(Duration.ofSeconds(1), Duration.ofMinutes(15),
                                Duration.ofHours(1), Duration.ofHours(24))),
                Clock.fixed(now, ZoneOffset.UTC),
                "worker-test")) {
            processor.process(deliveryId);
        }

        verify(attempts).finish(eq(lease), any(), eq(202), eq(null),
                eq("received [REDACTED]"), eq(null));
        assertThat(lease.deliveryId()).isEqualTo(deliveryId);
    }
}

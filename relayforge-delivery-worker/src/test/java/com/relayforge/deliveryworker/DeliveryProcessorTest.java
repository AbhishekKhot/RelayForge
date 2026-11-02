package com.relayforge.deliveryworker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.deliveryworker.http.DeliveryReferenceClient;
import com.relayforge.deliveryworker.http.WebhookRequestFactory;
import com.relayforge.deliveryworker.lease.DeliveryAttemptService;
import com.relayforge.deliveryworker.lease.DeliveryClaimService;
import com.relayforge.outbound.DestinationPolicy;
import com.relayforge.outbound.SafeWebhookClient;
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
import org.junit.jupiter.api.Test;

class DeliveryProcessorTest {
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
                organizationId, endpointId, 1));
        when(claims.claim(deliveryId, "worker-test", 2, 200)).thenReturn(Optional.of(lease));
        when(claims.owns(lease)).thenReturn(true);
        when(attempts.eventLocator(lease)).thenReturn("evt_test");
        when(attempts.reserve(lease, 3)).thenReturn(new DeliveryAttemptService.Attempt(
                UUID.randomUUID(), eventId, "evt_test", 2));
        when(references.endpoint(organizationId, endpointId)).thenReturn(json.readTree("""
                {"organization_id":"%s","endpoint_id":"%s","base_url":"http://localhost:18080",
                 "path":"/hook","query_params":{},"headers":{},"version":1,"timeout_ms":1000,
                 "max_attempts":3,"max_concurrency":2,"authentication":{"type":"NONE"},
                 "signing_secret":"%s"}
                """.formatted(organizationId, endpointId, secret)));
        when(references.payload(organizationId, "evt_test")).thenReturn(json.readTree("""
                {"organization_id":"%s","locator":"evt_test","event_type":"order.created",
                 "payload":{"order_id":"123"}}
                """.formatted(organizationId)));
        when(client.send(any(), any(), eq(Duration.ofSeconds(1)), any(), any()))
                .thenReturn(new SafeWebhookClient.Result(202, "received " + secret));

        try (DeliveryProcessor processor = new DeliveryProcessor(claims, attempts, references,
                new WebhookRequestFactory(json), policy, client, Clock.fixed(now, ZoneOffset.UTC),
                "worker-test")) {
            processor.process(deliveryId);
        }

        verify(attempts).finish(eq(lease), any(), eq(202), eq(null),
                eq("received [REDACTED]"), eq(null));
        assertThat(lease.deliveryId()).isEqualTo(deliveryId);
    }
}

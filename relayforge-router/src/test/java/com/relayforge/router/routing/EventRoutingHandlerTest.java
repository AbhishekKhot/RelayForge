package com.relayforge.router.routing;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.relayforge.contracts.ValidatedEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class EventRoutingHandlerTest {
    private final HttpEventReferenceVerifier verifier = mock(HttpEventReferenceVerifier.class);
    private final ProjectionService projections = mock(ProjectionService.class);
    private final RoutingService routing = mock(RoutingService.class);
    private final EventRoutingHandler handler = new EventRoutingHandler(verifier, projections, routing, "dlv-a");

    @Test
    void routesOnlyAfterVerificationAndProjectionReadiness() {
        UUID organization = UUID.randomUUID();
        VerifiedEvent event = new VerifiedEvent(organization, UUID.randomUUID(), "evt_test",
                LocalDate.parse("2026-11-02"), Instant.parse("2026-11-02T10:00:00Z"),
                "order.created", "ing-a", "dlv-a");
        ValidatedEnvelope envelope = mock(ValidatedEnvelope.class);
        when(envelope.payload()).thenReturn(new ObjectMapper().createObjectNode()
                .put("delivery_shard_id", "dlv-a"));
        when(verifier.verify(envelope)).thenReturn(event);

        handler.handle(envelope);

        var order = inOrder(verifier, projections, routing);
        order.verify(verifier).verify(envelope);
        order.verify(projections).ensureReady(organization);
        order.verify(routing).route(event);
    }

    @Test
    void doesNotRouteWhenVerificationFails() {
        ValidatedEnvelope envelope = mock(ValidatedEnvelope.class);
        when(envelope.payload()).thenReturn(new ObjectMapper().createObjectNode()
                .put("delivery_shard_id", "dlv-a"));
        when(verifier.verify(envelope)).thenThrow(new IllegalStateException("unavailable"));
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> handler.handle(envelope))
                .isInstanceOf(IllegalStateException.class);
        verifyNoInteractions(projections, routing);
    }

    @Test
    void skipsEventOwnedByAnotherDeliveryShard() {
        ValidatedEnvelope envelope = mock(ValidatedEnvelope.class);
        when(envelope.payload()).thenReturn(new ObjectMapper().createObjectNode()
                .put("delivery_shard_id", "dlv-b"));
        handler.handle(envelope);
        verifyNoInteractions(verifier, projections, routing);
    }
}

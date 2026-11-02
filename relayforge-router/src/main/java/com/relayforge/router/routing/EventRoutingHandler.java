package com.relayforge.router.routing;

import com.relayforge.consumer.MessageHandler;
import com.relayforge.contracts.ValidatedEnvelope;
import java.util.Objects;

public final class EventRoutingHandler implements MessageHandler {
    private final HttpEventReferenceVerifier verifier;
    private final ProjectionService projections;
    private final RoutingService routing;
    private final String deliveryShardId;

    public EventRoutingHandler(HttpEventReferenceVerifier verifier, ProjectionService projections,
            RoutingService routing, String deliveryShardId) {
        this.verifier = Objects.requireNonNull(verifier);
        this.projections = Objects.requireNonNull(projections);
        this.routing = Objects.requireNonNull(routing);
        this.deliveryShardId = Objects.requireNonNull(deliveryShardId);
    }

    @Override
    public void handle(ValidatedEnvelope envelope) {
        if (!deliveryShardId.equals(envelope.payload().path("delivery_shard_id").asText())) {
            return;
        }
        VerifiedEvent event = verifier.verify(envelope);
        projections.ensureReady(event.organizationId());
        routing.route(event);
    }
}

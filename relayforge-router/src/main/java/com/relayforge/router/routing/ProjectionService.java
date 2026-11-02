package com.relayforge.router.routing;

import com.fasterxml.jackson.databind.JsonNode;
import com.relayforge.contracts.ValidatedEnvelope;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.springframework.transaction.support.TransactionTemplate;

public final class ProjectionService {
    private final ProjectionRepository repository;
    private final TransactionTemplate transactions;
    private final ConfigurationSnapshotClient snapshots;

    public ProjectionService(ProjectionRepository repository, TransactionTemplate transactions,
            ConfigurationSnapshotClient snapshots) {
        this.repository = Objects.requireNonNull(repository);
        this.transactions = Objects.requireNonNull(transactions);
        this.snapshots = Objects.requireNonNull(snapshots);
    }

    public void ensureReady(UUID organizationId) {
        if (repository.ready(organizationId)) {
            return;
        }
        ConfigurationSnapshot snapshot = snapshots.load(organizationId);
        transactions.executeWithoutResult(status -> repository.replace(organizationId, snapshot));
        if (!repository.ready(organizationId)) {
            throw new IllegalStateException("Configuration projection is not ready");
        }
    }

    public void applyChange(ValidatedEnvelope envelope) {
        if (envelope == null || !"configuration.changed.v1".equals(envelope.messageType())) {
            throw new IllegalArgumentException("Invalid configuration change");
        }
        UUID organizationId = envelope.organizationId();
        JsonNode payload = envelope.payload();
        if (!organizationId.toString().equals(payload.path("organization_id").asText())) {
            throw new IllegalArgumentException("Invalid configuration tenant");
        }
        long revision = positiveLong(payload.path("source_revision"));
        JsonNode endpointNode = payload.path("endpoint");
        ConfigurationSnapshot.Endpoint endpoint = new ConfigurationSnapshot.Endpoint(
                uuid(endpointNode.path("endpoint_id")), status(endpointNode.path("status"),
                        List.of("ACTIVE", "PAUSED", "DISABLED")), positiveLong(endpointNode.path("version")));
        if (!endpoint.id().toString().equals(envelope.aggregateId())) {
            throw new IllegalArgumentException("Invalid configuration endpoint");
        }
        JsonNode subscriptionsNode = payload.path("subscriptions");
        if (!subscriptionsNode.isArray() || subscriptionsNode.size() > 10000) {
            throw new IllegalArgumentException("Invalid configuration subscriptions");
        }
        List<ConfigurationSnapshot.Subscription> subscriptions = new ArrayList<>();
        for (JsonNode item : subscriptionsNode) {
            UUID parent = uuid(item.path("endpoint_id"));
            if (!parent.equals(endpoint.id())) {
                throw new IllegalArgumentException("Invalid configuration subscription parent");
            }
            subscriptions.add(new ConfigurationSnapshot.Subscription(uuid(item.path("subscription_id")),
                    parent, text(item.path("event_type"), 200), status(item.path("status"),
                    List.of("ACTIVE", "PAUSED", "DELETED")), positiveLong(item.path("version")),
                    item.path("filter_config").isNull() || item.path("filter_config").isMissingNode()
                            ? null : item.path("filter_config").toString()));
        }
        ensureReady(organizationId);
        ProjectionRepository.ApplyResult result = transactions.execute(status -> repository.apply(
                organizationId, revision, endpoint, subscriptions));
        if (result == ProjectionRepository.ApplyResult.GAP) {
            ensureReady(organizationId);
            ProjectionRepository.ApplyResult replay = transactions.execute(status -> repository.apply(
                    organizationId, revision, endpoint, subscriptions));
            if (replay == ProjectionRepository.ApplyResult.GAP) {
                throw new IllegalStateException("Configuration revision gap remains");
            }
        }
    }

    private UUID uuid(JsonNode value) {
        String text = text(value, 36);
        UUID parsed = UUID.fromString(text);
        if (!parsed.toString().equals(text)) {
            throw new IllegalArgumentException("Invalid configuration identifier");
        }
        return parsed;
    }

    private long positiveLong(JsonNode value) {
        if (!value.isIntegralNumber() || !value.canConvertToLong() || value.longValue() < 1) {
            throw new IllegalArgumentException("Invalid configuration revision");
        }
        return value.longValue();
    }

    private String text(JsonNode value, int maximum) {
        if (!value.isTextual() || value.asText().isBlank() || value.asText().length() > maximum) {
            throw new IllegalArgumentException("Invalid configuration text");
        }
        return value.asText();
    }

    private String status(JsonNode value, List<String> allowed) {
        String result = text(value, 24);
        if (!allowed.contains(result)) {
            throw new IllegalArgumentException("Invalid configuration status");
        }
        return result;
    }
}

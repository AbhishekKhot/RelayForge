package com.relayforge.controlplane.endpoint;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public final class ConfigurationSnapshotService {
    private final ConfigurationSnapshotRepository repository;

    public ConfigurationSnapshotService(ConfigurationSnapshotRepository repository) {
        this.repository = Objects.requireNonNull(repository);
    }

    public Page<ConfigurationSnapshotRepository.EndpointState> endpoints(UUID organizationId,
            UUID afterId, int limit) {
        return page(organizationId, limit, () -> repository.endpoints(organizationId, afterId, limit + 1),
                ConfigurationSnapshotRepository.EndpointState::endpointId);
    }

    public Page<ConfigurationSnapshotRepository.SubscriptionState> subscriptions(UUID organizationId,
            UUID afterId, int limit) {
        return page(organizationId, limit, () -> repository.subscriptions(organizationId, afterId, limit + 1),
                ConfigurationSnapshotRepository.SubscriptionState::subscriptionId);
    }

    private <T> Page<T> page(UUID organizationId, int limit, Supplier<List<T>> fetch,
            Function<T, UUID> identity) {
        if (organizationId == null || limit < 1 || limit > 100) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        }
        long before = repository.revision(organizationId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
        List<T> rows = fetch.get();
        long after = repository.revision(organizationId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
        if (before != after) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
        }
        List<T> data = List.copyOf(rows.subList(0, Math.min(rows.size(), limit)));
        UUID cursor = rows.size() > limit ? identity.apply(data.getLast()) : null;
        return new Page<>(before, data, cursor);
    }

    public record Page<T>(@JsonProperty("source_revision") long sourceRevision,
            List<T> data, @JsonProperty("next_cursor") UUID nextCursor) {
    }
}

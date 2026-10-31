package com.relayforge.controlplane.endpoint;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public final class EndpointChangeRecorder {
    private final EndpointRepository repository;
    private final ObjectMapper json;

    public EndpointChangeRecorder(EndpointRepository repository, ObjectMapper json) {
        this.repository = repository;
        this.json = json;
    }

    public void record(UUID organizationId, UUID endpointId) {
        EndpointRepository.EndpointView endpoint = repository.findForOrganization(organizationId, endpointId)
                .orElseThrow(() -> new IllegalStateException("Updated endpoint was not found"));
        List<Map<String, Object>> subscriptions = repository.subscriptionsForEndpoint(organizationId, endpointId)
                .stream().map(subscription -> Map.<String, Object>of(
                        "subscription_id", subscription.id(), "endpoint_id", subscription.endpointId(),
                        "event_type", subscription.eventType(), "status", subscription.status(),
                        "version", subscription.version())).toList();
        long revision = repository.nextConfigurationRevision(organizationId);
        Map<String, Object> payload = Map.of(
                "organization_id", organizationId,
                "source_revision", revision,
                "endpoint", Map.of("endpoint_id", endpoint.id(), "status", endpoint.status(),
                        "version", endpoint.version()),
                "subscriptions", subscriptions);
        try {
            repository.insertConfigurationChange(organizationId, revision, endpointId,
                    json.writeValueAsString(payload));
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("Configuration snapshot could not be encoded", failure);
        }
    }
}

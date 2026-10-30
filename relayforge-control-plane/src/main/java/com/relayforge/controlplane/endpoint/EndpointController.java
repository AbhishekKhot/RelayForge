package com.relayforge.controlplane.endpoint;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.relayforge.apisupport.RequestIdFilter;
import com.relayforge.controlplane.security.ControlPlaneAuthFilter;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/endpoints")
@ConditionalOnProperty(name = "relayforge.database.username")
public final class EndpointController {
    private final EndpointRegistrationService registration;

    public EndpointController(EndpointRegistrationService registration) {
        this.registration = registration;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> create(@RequestBody EndpointRequest body,
            HttpServletRequest request) {
        UUID organizationId = (UUID) request.getAttribute(ControlPlaneAuthFilter.ORGANIZATION_ATTRIBUTE);
        EndpointRegistrationService.CreatedEndpoint created = registration.create(organizationId, body,
                RequestIdFilter.current(request));
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("data", Map.of(
                "endpoint_id", created.id(), "name", created.name(), "status", "ACTIVE",
                "event_types", created.eventTypes(), "created_at", created.createdAt()),
                "request_id", RequestIdFilter.current(request)));
    }

    @GetMapping("/{endpointId}")
    public Map<String, Object> get(@PathVariable UUID endpointId, HttpServletRequest request) {
        UUID organizationId = (UUID) request.getAttribute(ControlPlaneAuthFilter.ORGANIZATION_ATTRIBUTE);
        EndpointRepository.EndpointView endpoint = registration.get(organizationId, endpointId);
        return Map.of("data", Map.of("endpoint_id", endpoint.id(), "name", endpoint.name(),
                "status", endpoint.status(), "authentication", Map.of("type", endpoint.authType()),
                "created_at", endpoint.createdAt()), "request_id", RequestIdFilter.current(request));
    }

    public record EndpointRequest(String name, DestinationRequest destination,
            JsonNode authentication,
            @JsonProperty("event_types") List<String> eventTypes,
            @JsonProperty("delivery_policy") DeliveryPolicyRequest deliveryPolicy) {
        @Override
        public String toString() {
            return "EndpointRequest[name=[REDACTED], destination=[REDACTED], "
                    + "authentication=[REDACTED], eventTypes=[REDACTED], deliveryPolicy=[REDACTED]]";
        }
    }

    public record DestinationRequest(@JsonProperty("base_url") String baseUrl, String path,
            @JsonProperty("query_params") Map<String, String> queryParams,
            Map<String, String> headers, Map<String, Object> metadata) {
        @Override
        public String toString() {
            return "DestinationRequest[baseUrl=[REDACTED], path=[REDACTED], "
                    + "queryParams=[REDACTED], headers=[REDACTED], metadata=[REDACTED]]";
        }
    }

    public record DeliveryPolicyRequest(@JsonProperty("timeout_ms") Integer timeoutMs,
            @JsonProperty("max_attempts") Integer maxAttempts,
            @JsonProperty("max_concurrency") Integer maxConcurrency) {
    }
}

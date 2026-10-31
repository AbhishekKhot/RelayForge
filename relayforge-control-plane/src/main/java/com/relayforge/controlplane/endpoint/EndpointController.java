package com.relayforge.controlplane.endpoint;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.relayforge.apisupport.RequestIdFilter;
import com.relayforge.controlplane.security.ControlPlaneAuthFilter;
import jakarta.servlet.http.HttpServletRequest;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/endpoints")
@ConditionalOnProperty(name = "relayforge.database.username")
public final class EndpointController {
    private final EndpointRegistrationService registration;
    private final EndpointManagementService management;

    public EndpointController(EndpointRegistrationService registration, EndpointManagementService management) {
        this.registration = registration;
        this.management = management;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> create(@RequestBody EndpointRequest body,
            HttpServletRequest request) {
        UUID organizationId = (UUID) request.getAttribute(ControlPlaneAuthFilter.ORGANIZATION_ATTRIBUTE);
        EndpointRegistrationService.CreatedEndpoint created = registration.create(organizationId, body,
                RequestIdFilter.current(request));
        return ResponseEntity.status(HttpStatus.CREATED).eTag("\"1\"").body(Map.of("data", Map.of(
                "endpoint_id", created.id(), "name", created.name(), "status", "ACTIVE",
                "event_types", created.eventTypes(), "created_at", created.createdAt(), "version", 1),
                "request_id", RequestIdFilter.current(request)));
    }

    @GetMapping
    public Map<String, Object> list(@RequestParam(required = false) String limit,
            @RequestParam(required = false) String cursor, HttpServletRequest request) {
        UUID organizationId = organizationId(request);
        var page = management.endpoints(organizationId, EndpointQueryParameters.page(limit, cursor));
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("data", page.data().stream().map(EndpointController::view).toList());
        response.put("next_cursor", page.nextCursor());
        response.put("request_id", RequestIdFilter.current(request));
        return response;
    }

    @GetMapping("/{endpointId}")
    public ResponseEntity<Map<String, Object>> get(@PathVariable UUID endpointId, HttpServletRequest request) {
        EndpointRepository.EndpointView endpoint = management.get(organizationId(request), endpointId);
        return ResponseEntity.ok().eTag(etag(endpoint.version()))
                .body(Map.of("data", view(endpoint), "request_id", RequestIdFilter.current(request)));
    }

    @PatchMapping("/{endpointId}")
    public ResponseEntity<Map<String, Object>> patch(@PathVariable UUID endpointId,
            @RequestHeader(value = "If-Match", required = false) String ifMatch,
            @RequestBody PatchRequest body, HttpServletRequest request) {
        var updated = management.patch(organizationId(request), endpointId,
                EndpointQueryParameters.version(ifMatch), body, RequestIdFilter.current(request));
        return ResponseEntity.ok().eTag(etag(updated.version()))
                .body(Map.of("data", view(updated), "request_id", RequestIdFilter.current(request)));
    }

    @PostMapping("/{endpointId}/rotate-secret")
    public ResponseEntity<Map<String, Object>> rotateSecret(@PathVariable UUID endpointId,
            @RequestHeader(value = "If-Match", required = false) String ifMatch,
            HttpServletRequest request) {
        var rotated = management.rotateSecret(organizationId(request), endpointId,
                EndpointQueryParameters.version(ifMatch), RequestIdFilter.current(request));
        return ResponseEntity.ok().eTag(etag(rotated.version())).body(Map.of("data", Map.of(
                "endpoint_id", rotated.endpointId(), "signing_secret", rotated.secret(),
                "version", rotated.version()), "request_id", RequestIdFilter.current(request)));
    }

    private static Map<String, Object> view(EndpointRepository.EndpointView endpoint) {
        return Map.of("endpoint_id", endpoint.id(), "name", endpoint.name(),
                "status", endpoint.status(), "authentication", Map.of("type", endpoint.authType()),
                "created_at", endpoint.createdAt(), "version", endpoint.version());
    }

    private UUID organizationId(HttpServletRequest request) {
        return (UUID) request.getAttribute(ControlPlaneAuthFilter.ORGANIZATION_ATTRIBUTE);
    }

    private String etag(long version) {
        return "\"" + version + "\"";
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

    public record PatchRequest(String name, String status, DestinationRequest destination,
            JsonNode authentication, @JsonProperty("delivery_policy") DeliveryPolicyRequest deliveryPolicy) {
        @Override
        public String toString() {
            return "PatchRequest[name=[REDACTED], status=" + status + ", destination=[REDACTED], "
                    + "authentication=[REDACTED], deliveryPolicy=[REDACTED]]";
        }
    }
}

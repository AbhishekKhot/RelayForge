package com.relayforge.controlplane.endpoint;

import com.relayforge.apisupport.RequestIdFilter;
import com.relayforge.controlplane.security.ControlPlaneAuthFilter;
import jakarta.servlet.http.HttpServletRequest;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/subscriptions")
@ConditionalOnProperty(name = "relayforge.database.username")
public final class SubscriptionController {
    private final EndpointManagementService management;

    public SubscriptionController(EndpointManagementService management) {
        this.management = management;
    }

    @GetMapping
    public Map<String, Object> list(@RequestParam(required = false) String limit,
            @RequestParam(required = false) String cursor, HttpServletRequest request) {
        UUID organizationId = (UUID) request.getAttribute(ControlPlaneAuthFilter.ORGANIZATION_ATTRIBUTE);
        var page = management.subscriptions(organizationId, EndpointQueryParameters.page(limit, cursor));
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("data", page.data().stream().map(subscription -> Map.of(
                "subscription_id", subscription.id(), "endpoint_id", subscription.endpointId(),
                "event_type", subscription.eventType(), "status", subscription.status(),
                "version", subscription.version())).toList());
        response.put("next_cursor", page.nextCursor());
        response.put("request_id", RequestIdFilter.current(request));
        return response;
    }
}

package com.relayforge.controlplane.security;

import com.relayforge.apisupport.RequestIdFilter;
import com.relayforge.controlplane.organization.OrganizationService;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Map;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@ConditionalOnProperty(name = "relayforge.database.username")
public final class AuthCheckController {
    private final OrganizationService organizations;

    public AuthCheckController(OrganizationService organizations) {
        this.organizations = organizations;
    }

    @GetMapping("/v1/auth/check")
    public Map<String, Object> check(HttpServletRequest request) {
        UUID organizationId = (UUID) request.getAttribute(ControlPlaneAuthFilter.ORGANIZATION_ATTRIBUTE);
        return Map.of("organization_id", organizationId,
                "events_per_minute", organizations.eventRateLimitFor(organizationId),
                "request_id", RequestIdFilter.current(request));
    }
}

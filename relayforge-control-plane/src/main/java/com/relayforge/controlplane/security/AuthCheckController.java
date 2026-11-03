package com.relayforge.controlplane.security;

import com.relayforge.apisupport.RequestIdFilter;
import com.relayforge.controlplane.organization.QuotaConfigurationService;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Map;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@ConditionalOnProperty(name = "relayforge.database.username")
public final class AuthCheckController {
    private final QuotaConfigurationService quotas;

    public AuthCheckController(QuotaConfigurationService quotas) {
        this.quotas = quotas;
    }

    @GetMapping("/v1/auth/check")
    public Map<String, Object> check(HttpServletRequest request) {
        UUID organizationId = (UUID) request.getAttribute(ControlPlaneAuthFilter.ORGANIZATION_ATTRIBUTE);
        Map<String, Integer> resolved = quotas.forOrganization(organizationId);
        return Map.of("organization_id", organizationId,
                "key_id", request.getAttribute(ControlPlaneAuthFilter.KEY_ATTRIBUTE),
                "events_per_minute", resolved.get("events_per_minute"),
                "quotas", resolved,
                "request_id", RequestIdFilter.current(request));
    }
}

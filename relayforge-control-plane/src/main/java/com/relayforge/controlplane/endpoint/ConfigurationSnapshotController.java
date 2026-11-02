package com.relayforge.controlplane.endpoint;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/internal/v1/organizations/{organizationId}/configuration")
@ConditionalOnProperty(name = "relayforge.database.username")
public final class ConfigurationSnapshotController {
    private final ConfigurationSnapshotService service;
    private final byte[] credential;

    public ConfigurationSnapshotController(ConfigurationSnapshotService service,
            @Value("${relayforge.router.config-credential}") String credential) {
        this.service = service;
        this.credential = credential.getBytes(StandardCharsets.UTF_8);
        if (this.credential.length < 32 || this.credential.length > 256) {
            throw new IllegalStateException("A strong router configuration credential is required");
        }
    }

    @GetMapping("/endpoints")
    public ConfigurationSnapshotService.Page<ConfigurationSnapshotRepository.EndpointState> endpoints(
            @PathVariable UUID organizationId, @RequestParam(required = false) UUID after,
            @RequestParam(defaultValue = "100") int limit,
            @RequestHeader(value = "X-RelayForge-Router-Config-Credential", required = false)
            String supplied) {
        authorize(supplied);
        return service.endpoints(organizationId, after, limit);
    }

    @GetMapping("/subscriptions")
    public ConfigurationSnapshotService.Page<ConfigurationSnapshotRepository.SubscriptionState> subscriptions(
            @PathVariable UUID organizationId, @RequestParam(required = false) UUID after,
            @RequestParam(defaultValue = "100") int limit,
            @RequestHeader(value = "X-RelayForge-Router-Config-Credential", required = false)
            String supplied) {
        authorize(supplied);
        return service.subscriptions(organizationId, after, limit);
    }

    private void authorize(String supplied) {
        if (supplied == null || supplied.length() > 256 || !MessageDigest.isEqual(credential,
                supplied.getBytes(StandardCharsets.UTF_8))) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
        }
    }
}

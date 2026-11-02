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
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/internal/v1/organizations/{organizationId}/endpoints")
@ConditionalOnProperty(name = {"relayforge.database.username", "relayforge.delivery.config-credential"})
public final class DeliveryEndpointController {
    private final DeliveryEndpointService service;
    private final byte[] credential;

    public DeliveryEndpointController(DeliveryEndpointService service,
            @Value("${relayforge.delivery.config-credential}") String credential) {
        this.service = service;
        this.credential = credential.getBytes(StandardCharsets.UTF_8);
        if (this.credential.length < 32 || this.credential.length > 256) {
            throw new IllegalStateException("A strong delivery configuration credential is required");
        }
    }

    @GetMapping("/{endpointId}/delivery")
    public DeliveryEndpointService.Configuration find(@PathVariable UUID organizationId,
            @PathVariable UUID endpointId,
            @RequestHeader(value = "X-RelayForge-Delivery-Config-Credential", required = false)
            String supplied) {
        if (supplied == null || supplied.length() > 256 || !MessageDigest.isEqual(credential,
                supplied.getBytes(StandardCharsets.UTF_8))) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
        }
        return service.find(organizationId, endpointId);
    }
}

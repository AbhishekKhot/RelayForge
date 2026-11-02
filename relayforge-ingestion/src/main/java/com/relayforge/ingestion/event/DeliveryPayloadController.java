package com.relayforge.ingestion.event;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@ConditionalOnProperty(name = {"relayforge.database.username", "relayforge.delivery.payload-credential"})
public final class DeliveryPayloadController {
    private final DeliveryPayloadLookupService service;
    private final byte[] credential;

    public DeliveryPayloadController(DeliveryPayloadLookupService service,
            @Value("${relayforge.delivery.payload-credential}") String credential) {
        this.service = service;
        this.credential = credential.getBytes(StandardCharsets.UTF_8);
        if (this.credential.length < 32 || this.credential.length > 256) {
            throw new IllegalStateException("A strong delivery payload credential is required");
        }
    }

    @GetMapping("/internal/v1/events/{locator}/payload")
    public DeliveryPayloadLookupService.Payload find(@PathVariable String locator,
            @RequestParam("organization_id") UUID organizationId,
            @RequestHeader(value = "X-RelayForge-Delivery-Payload-Credential", required = false)
            String supplied) {
        if (supplied == null || supplied.length() > 256 || !MessageDigest.isEqual(credential,
                supplied.getBytes(StandardCharsets.UTF_8))) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
        }
        return service.find(organizationId, locator);
    }
}

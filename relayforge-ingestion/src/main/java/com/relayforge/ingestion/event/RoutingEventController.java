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
@ConditionalOnProperty(name = "relayforge.database.username")
public final class RoutingEventController {
    private final RoutingEventLookupService service;
    private final byte[] credential;

    public RoutingEventController(RoutingEventLookupService service,
            @Value("${relayforge.router.event-credential}") String credential) {
        this.service = service;
        this.credential = credential.getBytes(StandardCharsets.UTF_8);
        if (this.credential.length < 32 || this.credential.length > 256) {
            throw new IllegalStateException("A strong router event credential is required");
        }
    }

    @GetMapping("/internal/v1/events/{locator}/routing")
    public RoutingEventLookupService.VerifiedEvent find(@PathVariable String locator,
            @RequestParam("organization_id") UUID organizationId,
            @RequestHeader(value = "X-RelayForge-Router-Event-Credential", required = false) String supplied) {
        if (supplied == null || supplied.length() > 256 || !MessageDigest.isEqual(credential,
                supplied.getBytes(StandardCharsets.UTF_8))) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
        }
        return service.find(organizationId, locator);
    }
}

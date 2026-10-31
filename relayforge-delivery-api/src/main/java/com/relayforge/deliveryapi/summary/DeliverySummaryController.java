package com.relayforge.deliveryapi.summary;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDate;
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
public final class DeliverySummaryController {
    private final DeliverySummaryRepository repository;
    private final byte[] credential;

    public DeliverySummaryController(DeliverySummaryRepository repository,
            @Value("${relayforge.event-summary.credential}") String credential) {
        this.repository = repository;
        this.credential = credential.getBytes(StandardCharsets.UTF_8);
        if (this.credential.length < 32 || this.credential.length > 256) {
            throw new IllegalStateException("A strong event summary credential is required");
        }
    }

    @GetMapping("/internal/v1/events/{eventId}/summary")
    public DeliverySummary find(@PathVariable UUID eventId,
            @RequestParam("organization_id") UUID organizationId,
            @RequestParam("accepted_day") LocalDate acceptedDay,
            @RequestHeader(value = "X-RelayForge-Event-Summary-Credential", required = false) String supplied) {
        if (supplied == null || supplied.length() > 256 || !MessageDigest.isEqual(credential,
                supplied.getBytes(StandardCharsets.UTF_8))) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
        }
        return repository.find(organizationId, acceptedDay, eventId).orElseGet(DeliverySummary::pending);
    }
}

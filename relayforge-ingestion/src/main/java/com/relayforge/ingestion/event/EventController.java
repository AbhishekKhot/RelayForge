package com.relayforge.ingestion.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.apisupport.RequestIdFilter;
import com.relayforge.ingestion.event.OrganizationAuthenticator.AuthenticatedOrganization;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/v1/events")
@ConditionalOnProperty(name = "relayforge.database.username")
public final class EventController {
    private final OrganizationAuthenticator authenticator;
    private final EventIngestionService ingestion;
    private final ObjectMapper json;
    private final EventRequestParser parser;
    private final int maxPayloadBytes;

    public EventController(OrganizationAuthenticator authenticator, EventIngestionService ingestion,
            ObjectMapper json, EventRequestParser parser, org.springframework.core.env.Environment environment) {
        this.authenticator = authenticator;
        this.ingestion = ingestion;
        this.json = json;
        this.parser = parser;
        this.maxPayloadBytes = environment.getProperty("relayforge.api.max-event-bytes", Integer.class, 262144);
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> publish(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            HttpServletRequest request) throws IOException {
        AuthenticatedOrganization organization = authenticator.authenticate(authorization);
        EventRequestParser.ParsedEvent body = parser.parse(request.getInputStream(), maxPayloadBytes);
        EventPayload payload;
        try {
            payload = EventPayload.validate(json, body.eventType(), body.schemaVersion(),
                    body.occurredAt(), body.data(), maxPayloadBytes);
        } catch (IllegalArgumentException invalid) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        }
        EventIngestionService.AcceptedEvent accepted = ingestion.accept(organization, idempotencyKey, payload);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of(
                "data", Map.of("event_id", accepted.id(), "event_type", accepted.eventType(),
                        "status", "ACCEPTED", "accepted_at", accepted.acceptedAt()),
                "request_id", RequestIdFilter.current(request)));
    }

}

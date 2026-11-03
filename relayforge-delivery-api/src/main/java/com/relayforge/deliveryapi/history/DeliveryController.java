package com.relayforge.deliveryapi.history;

import com.relayforge.apisupport.RequestIdFilter;
import com.relayforge.deliveryapi.replay.DeliveryReplayService;
import jakarta.servlet.http.HttpServletRequest;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.ResponseEntity;

@RestController
@ConditionalOnProperty(name = "relayforge.database.username")
public final class DeliveryController {
    private final DeliveryApiAuthenticator authenticator;
    private final DeliveryQueryService queries;
    private final DeliveryReplayService replays;

    public DeliveryController(DeliveryApiAuthenticator authenticator, DeliveryQueryService queries,
            DeliveryReplayService replays) {
        this.authenticator = authenticator;
        this.queries = queries;
        this.replays = replays;
    }

    @GetMapping("/v1/deliveries")
    public Map<String, Object> list(@RequestHeader(value = "Authorization", required = false)
            String authorization, @RequestParam(required = false) String status,
            @RequestParam(value = "endpoint_id", required = false) String endpointId,
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to,
            @RequestParam(required = false) String limit,
            @RequestParam(required = false) String cursor, HttpServletRequest request) {
        UUID organization = authenticator.authenticate(authorization);
        DeliveryQueryService.PageResult result = queries.list(organization, status, endpointId,
                from, to, limit, cursor);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("data", result.data());
        response.put("next_cursor", result.nextCursor());
        response.put("request_id", RequestIdFilter.current(request));
        return response;
    }

    @GetMapping("/v1/deliveries/{deliveryId}")
    public Map<String, Object> detail(@RequestHeader(value = "Authorization", required = false)
            String authorization, @PathVariable UUID deliveryId, HttpServletRequest request) {
        UUID organization = authenticator.authenticate(authorization);
        DeliveryQueryService.DetailView found = queries.detail(organization, deliveryId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
        return Map.of("data", found, "request_id", RequestIdFilter.current(request));
    }

    @PostMapping("/v1/deliveries/{deliveryId}/replay")
    public ResponseEntity<Map<String, Object>> replay(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @RequestHeader(value = "Idempotency-Key", required = false) String key,
            @PathVariable UUID deliveryId, @RequestBody(required = false) ReplayRequest body,
            HttpServletRequest request) {
        var actor = authenticator.authenticateReplay(authorization);
        var result = replays.replay(actor.organizationId(), actor.keyId(), deliveryId, key,
                body == null ? null : body.reason(), RequestIdFilter.current(request),
                actor.maxReplayGenerations());
        return ResponseEntity.accepted().body(Map.of("data", result,
                "request_id", RequestIdFilter.current(request)));
    }

    public record ReplayRequest(String reason) {
    }
}

package com.relayforge.controlplane.organization;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.relayforge.apisupport.RequestIdFilter;
import com.relayforge.controlplane.security.ControlPlaneAuthFilter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1")
@ConditionalOnProperty(name = "relayforge.database.username")
public final class ProvisioningController {
    private final OrganizationService organizations;

    public ProvisioningController(OrganizationService organizations) {
        this.organizations = organizations;
    }

    @PostMapping("/organizations")
    public ResponseEntity<Map<String, Object>> createOrganization(@Valid @RequestBody CreateOrganizationRequest body,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            HttpServletRequest request) {
        OrganizationService.CreatedOrganization created = organizations.createOrganization(
                body.name(), body.planCode(), body.quotaOverrides(), idempotencyKey,
                RequestIdFilter.current(request));
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "data", Map.of("organization_id", created.id(), "name", created.name(),
                        "api_key", created.apiKey(), "api_key_warning", "Shown once. Store it securely."),
                "request_id", RequestIdFilter.current(request)));
    }

    @GetMapping("/organizations/recover")
    public Map<String, Object> recoverOrganization(
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            HttpServletRequest request) {
        OrganizationRepository.ProvisioningRequest recovered = organizations.recoverOrganization(idempotencyKey);
        return Map.of("data", Map.of("organization_id", recovered.organizationId(),
                        "key_id", recovered.keyId()),
                "request_id", RequestIdFilter.current(request));
    }

    @GetMapping("/organizations/{organizationId}")
    public Map<String, Object> organization(@PathVariable UUID organizationId, HttpServletRequest request) {
        UUID authenticatedId = (UUID) request.getAttribute(ControlPlaneAuthFilter.ORGANIZATION_ATTRIBUTE);
        OrganizationRepository.OrganizationView owner = organizations.organizationForOwner(
                organizationId, authenticatedId);
        return Map.of("data", Map.of("organization_id", owner.id(), "name", owner.name(),
                        "plan_code", owner.planCode()),
                "request_id", RequestIdFilter.current(request));
    }

    @PostMapping("/users")
    public ResponseEntity<Map<String, Object>> createUser(@Valid @RequestBody CreateUserRequest body,
            HttpServletRequest request) {
        UUID id = organizations.createUser(body.organizationId(), body.email(), body.name(), body.metadata(),
                RequestIdFilter.current(request));
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "data", Map.of("user_id", id, "organization_id", body.organizationId()),
                "request_id", RequestIdFilter.current(request)));
    }

    @PostMapping("/api-keys/{keyId}/rotate")
    public ResponseEntity<Map<String, Object>> rotate(@PathVariable String keyId,
            @RequestBody(required = false) RotateKeyRequest body, HttpServletRequest request) {
        OrganizationService.RotatedKey replacement = organizations.rotateKey(keyId,
                body == null ? null : body.scopes(), RequestIdFilter.current(request));
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "data", Map.of("organization_id", replacement.organizationId(), "api_key", replacement.apiKey(),
                        "api_key_warning", "Shown once. Store it securely."),
                "request_id", RequestIdFilter.current(request)));
    }

    @PostMapping("/api-keys/{keyId}/revoke")
    public ResponseEntity<Void> revoke(@PathVariable String keyId, HttpServletRequest request) {
        organizations.revokeKey(keyId, RequestIdFilter.current(request));
        return ResponseEntity.noContent().build();
    }

    public record CreateOrganizationRequest(@NotBlank @Size(max = 160) String name,
            @JsonProperty("plan_code") String planCode,
            @JsonProperty("quota_overrides") Map<String, Object> quotaOverrides) {
    }

    public record CreateUserRequest(@JsonProperty("organization_id") @NotNull UUID organizationId,
            @NotBlank @Email @Size(max = 320) String email,
            @NotBlank @Size(max = 160) String name,
            Map<String, Object> metadata) {
    }

    public record RotateKeyRequest(List<String> scopes) {
    }
}

package com.relayforge.controlplane.organization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.controlplane.security.ApiKeyCodec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

public final class OrganizationService {
    private static final Map<String, Integer> FREE_DEFAULTS = Map.of(
            "events_per_minute", 600,
            "max_active_endpoints", 25,
            "max_in_flight_deliveries", 100);
    private static final Map<String, Integer> PLATFORM_MAXIMUMS = Map.of(
            "events_per_minute", 60000,
            "max_active_endpoints", 1000,
            "max_in_flight_deliveries", 10000);
    private static final Set<String> ALLOWED_SCOPES = Set.of(
            "events:write", "events:read", "endpoints:read", "endpoints:write",
            "deliveries:read", "deliveries:replay", "analytics:read", "organizations:read");

    private final OrganizationRepository repository;
    private final ApiKeyCodec codec;
    private final TransactionTemplate transactions;
    private final ObjectMapper json;
    private final Clock clock;

    public OrganizationService(OrganizationRepository repository, ApiKeyCodec codec,
            TransactionTemplate transactions, ObjectMapper json, Clock clock) {
        this.repository = repository;
        this.codec = codec;
        this.transactions = transactions;
        this.json = json;
        this.clock = clock;
    }

    public CreatedOrganization createOrganization(String name, String planCode,
            Map<String, Object> overrides, String idempotencyKey, String requestId) {
        String validatedName = requireText(name, 160);
        byte[] requestHash = requestHash(idempotencyKey);
        String selectedPlan = planCode == null ? "FREE" : planCode;
        if (!"FREE".equals(selectedPlan)) {
            throw invalidRequest();
        }
        Map<String, Integer> quotas = quotas(overrides);
        ApiKeyCodec.IssuedKey key = codec.issue();
        UUID organizationId = UUID.randomUUID();
        try {
            return transactions.execute(status -> {
                if (repository.findProvisioningRequest(requestHash).isPresent()) {
                    throw new ResponseStatusException(HttpStatus.CONFLICT);
                }
                repository.insertOrganization(organizationId, validatedName, selectedPlan, toJson(quotas));
                repository.insertKey(UUID.randomUUID(), organizationId, key,
                        List.of("events:write", "organizations:read"));
                repository.insertProvisioningRequest(requestHash, organizationId, key.keyId());
                repository.insertAudit(organizationId, "organization.created", "organization",
                        organizationId, requestId, "{}");
                return new CreatedOrganization(organizationId, validatedName, key.plaintext());
            });
        } catch (DuplicateKeyException duplicate) {
            throw new ResponseStatusException(HttpStatus.CONFLICT);
        }
    }

    public OrganizationRepository.ProvisioningRequest recoverOrganization(String idempotencyKey) {
        return repository.findProvisioningRequest(requestHash(idempotencyKey))
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
    }

    public UUID createUser(UUID organizationId, String email, String name,
            Map<String, Object> metadata, String requestId) {
        String validatedName = requireText(name, 160);
        String validatedEmail = requireText(email, 320).toLowerCase(java.util.Locale.ROOT);
        UUID userId = UUID.randomUUID();
        try {
            return transactions.execute(status -> {
                if (!repository.activeOrganizationExists(organizationId)) {
                    throw new ResponseStatusException(HttpStatus.NOT_FOUND);
                }
                repository.insertUser(userId, organizationId, validatedEmail, validatedName,
                        toJson(metadata == null ? Map.of() : metadata));
                repository.insertAudit(organizationId, "user.created", "user", userId, requestId, "{}");
                return userId;
            });
        } catch (DuplicateKeyException duplicate) {
            throw new ResponseStatusException(HttpStatus.CONFLICT);
        }
    }

    public RotatedKey rotateKey(String oldKeyId, List<String> requestedScopes, String requestId) {
        List<String> scopes = validateScopes(requestedScopes);
        ApiKeyCodec.IssuedKey replacement = codec.issue();
        return transactions.execute(status -> {
            OrganizationRepository.StoredKey old = repository.findKey(oldKeyId)
                    .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
            if (old.revokedAt() != null || !"ACTIVE".equals(old.organizationStatus())
                    || old.expiresAt() != null && !old.expiresAt().isAfter(clock.instant())
                    || !repository.revokeKey(old.id())) {
                throw new ResponseStatusException(HttpStatus.NOT_FOUND);
            }
            UUID newId = UUID.randomUUID();
            repository.insertKey(newId, old.organizationId(), replacement,
                    scopes.isEmpty() ? old.scopes() : scopes);
            repository.insertAudit(old.organizationId(), "api_key.rotated", "api_key", newId,
                    requestId, "{}");
            return new RotatedKey(old.organizationId(), replacement.plaintext());
        });
    }

    public void revokeKey(String keyId, String requestId) {
        transactions.executeWithoutResult(status -> {
            OrganizationRepository.StoredKey key = repository.findKey(keyId)
                    .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
            if (!repository.revokeKey(key.id())) {
                throw new ResponseStatusException(HttpStatus.NOT_FOUND);
            }
            repository.insertAudit(key.organizationId(), "api_key.revoked", "api_key", key.id(), requestId, "{}");
        });
    }

    public Authentication authenticate(String plaintext, String requiredScope) {
        String keyId = codec.keyId(plaintext).orElse(null);
        if (keyId == null) {
            return Authentication.invalid();
        }
        OrganizationRepository.StoredKey key = repository.findKey(keyId).orElse(null);
        if (key == null || !codec.verify(plaintext, key.digest()) || key.revokedAt() != null
                || !"ACTIVE".equals(key.organizationStatus())
                || key.expiresAt() != null && !key.expiresAt().isAfter(clock.instant())) {
            return Authentication.invalid();
        }
        if (!key.scopes().contains(requiredScope)) {
            return Authentication.forbidden();
        }
        if (!repository.touchActiveKey(key.id(), requiredScope)) {
            return Authentication.invalid();
        }
        return Authentication.accepted(key.organizationId());
    }

    public OrganizationRepository.OrganizationView organizationForOwner(UUID requestedId, UUID authenticatedId) {
        return repository.findOrganizationForOwner(requestedId, authenticatedId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
    }

    public int eventRateLimitFor(UUID authenticatedId) {
        return repository.findActiveEventRateLimit(authenticatedId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE));
    }

    private Map<String, Integer> quotas(Map<String, Object> overrides) {
        Map<String, Integer> result = new LinkedHashMap<>(FREE_DEFAULTS);
        if (overrides != null) {
            for (Map.Entry<String, Object> entry : overrides.entrySet()) {
                Integer maximum = PLATFORM_MAXIMUMS.get(entry.getKey());
                if (maximum == null || !(entry.getValue() instanceof Integer value)
                        || value < 1 || value > maximum) {
                    throw invalidRequest();
                }
                result.put(entry.getKey(), value);
            }
        }
        return result;
    }

    private List<String> validateScopes(List<String> scopes) {
        if (scopes == null) {
            return List.of();
        }
        if (scopes.isEmpty() || scopes.size() > ALLOWED_SCOPES.size()
                || !ALLOWED_SCOPES.containsAll(scopes) || Set.copyOf(scopes).size() != scopes.size()) {
            throw invalidRequest();
        }
        return List.copyOf(scopes);
    }

    private String requireText(String value, int maximumLength) {
        if (value == null || value.isBlank() || value.length() > maximumLength) {
            throw invalidRequest();
        }
        return value.trim();
    }

    private String toJson(Object value) {
        try {
            return json.writeValueAsString(value);
        } catch (JsonProcessingException invalid) {
            throw invalidRequest();
        }
    }

    private byte[] requestHash(String key) {
        if (key == null || !key.matches("[A-Za-z0-9_-]{16,100}")) {
            throw invalidRequest();
        }
        try {
            return MessageDigest.getInstance("SHA-256").digest(key.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("SHA-256 is unavailable", unavailable);
        }
    }

    private ResponseStatusException invalidRequest() {
        return new ResponseStatusException(HttpStatus.BAD_REQUEST);
    }

    public record CreatedOrganization(UUID id, String name, String apiKey) {
        @Override
        public String toString() {
            return "CreatedOrganization[id=" + id + ", apiKey=[REDACTED]]";
        }
    }

    public record RotatedKey(UUID organizationId, String apiKey) {
        @Override
        public String toString() {
            return "RotatedKey[organizationId=" + organizationId + ", apiKey=[REDACTED]]";
        }
    }

    public record Authentication(HttpStatus status, UUID organizationId) {
        static Authentication accepted(UUID organizationId) {
            return new Authentication(HttpStatus.OK, organizationId);
        }

        static Authentication invalid() {
            return new Authentication(HttpStatus.UNAUTHORIZED, null);
        }

        static Authentication forbidden() {
            return new Authentication(HttpStatus.FORBIDDEN, null);
        }
    }
}

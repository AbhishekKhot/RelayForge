package com.relayforge.controlplane.organization;

import com.relayforge.controlplane.security.ApiKeyCodec;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface OrganizationRepository {
    void insertOrganization(UUID id, String name, String planCode, String quotaJson);

    void insertProvisioningRequest(byte[] requestHash, UUID organizationId, String keyId);

    Optional<ProvisioningRequest> findProvisioningRequest(byte[] requestHash);

    void insertKey(UUID id, UUID organizationId, ApiKeyCodec.IssuedKey key, List<String> scopes);

    Optional<StoredKey> findKey(String keyId);

    boolean touchActiveKey(UUID id, String requiredScope);

    boolean activeOrganizationExists(UUID organizationId);

    Optional<OrganizationView> findOrganizationForOwner(UUID requestedId, UUID authenticatedId);

    void insertUser(UUID id, UUID organizationId, String email, String name, String metadataJson);

    boolean revokeKey(UUID id);

    void insertAudit(UUID organizationId, String action, String resourceType, UUID resourceId,
            String requestId, String detailsJson);

    record StoredKey(UUID id, UUID organizationId, String keyId, byte[] digest, List<String> scopes,
            String organizationStatus, Instant expiresAt, Instant revokedAt) {
    }

    record OrganizationView(UUID id, String name, String planCode) {
    }

    record ProvisioningRequest(UUID organizationId, String keyId) {
    }
}

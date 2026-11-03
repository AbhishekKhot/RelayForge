package com.relayforge.controlplane.organization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

public final class QuotaConfigurationService {
    private static final TypeReference<Map<String, Integer>> QUOTA_TYPE = new TypeReference<>() { };
    private final OrganizationRepository repository;
    private final QuotaCache cache;
    private final ObjectMapper json;
    private final TransactionTemplate transactions;

    public QuotaConfigurationService(OrganizationRepository repository, QuotaCache cache,
            ObjectMapper json, TransactionTemplate transactions) {
        this.repository = Objects.requireNonNull(repository);
        this.cache = Objects.requireNonNull(cache);
        this.json = Objects.requireNonNull(json);
        this.transactions = Objects.requireNonNull(transactions);
    }

    public Map<String, Integer> forOrganization(UUID organizationId) {
        try {
            long revision = repository.findActiveQuotaRevision(organizationId)
                    .orElseThrow(() -> new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE));
            try {
                var cached = cache.find(organizationId, revision);
                if (cached.isPresent()) {
                    return QuotaPolicy.resolve("FREE", cached.get());
                }
            } catch (DataAccessException unavailable) {
                // PostgreSQL remains authoritative when Redis is unavailable.
            }
            OrganizationRepository.QuotaSnapshot stored = repository.findActiveQuotaSnapshot(organizationId)
                    .orElseThrow(() -> new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE));
            Map<String, Integer> resolved = resolve(stored);
            try {
                cache.put(organizationId, stored.revision(), resolved);
            } catch (DataAccessException unavailable) {
                // Redis cache failure does not disable PostgreSQL-backed quota lookup.
            }
            return resolved;
        } catch (DataAccessException unavailable) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
        }
    }

    public Map<String, Integer> replace(UUID organizationId, Map<String, Object> overrides) {
        try {
            return transactions.execute(status -> {
                OrganizationRepository.QuotaSnapshot stored = repository.findActiveQuotaSnapshot(organizationId)
                        .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
                Map<String, Integer> resolved;
                try {
                    resolved = QuotaPolicy.resolve(stored.planCode(), overrides);
                    if (!repository.updateActiveQuotas(organizationId, json.writeValueAsString(resolved))) {
                        throw new ResponseStatusException(HttpStatus.NOT_FOUND);
                    }
                } catch (IllegalArgumentException | JsonProcessingException invalid) {
                    throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
                }
                return resolved;
            });
        } catch (DataAccessException unavailable) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
        }
    }

    private Map<String, Integer> resolve(OrganizationRepository.QuotaSnapshot stored) {
        try {
            return QuotaPolicy.resolve(stored.planCode(), json.readValue(stored.configJson(), QUOTA_TYPE));
        } catch (JsonProcessingException | IllegalArgumentException invalid) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
        }
    }
}

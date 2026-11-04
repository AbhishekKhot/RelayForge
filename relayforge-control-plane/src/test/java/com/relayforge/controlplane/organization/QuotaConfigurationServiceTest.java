package com.relayforge.controlplane.organization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

class QuotaConfigurationServiceTest {
    @Test
    void refreshesVersionedCacheAfterQuotaChange() {
        UUID organization = UUID.randomUUID();
        OrganizationRepository repository = mock(OrganizationRepository.class);
        Map<Long, Map<String, Integer>> values = new HashMap<>();
        QuotaCache cache = new QuotaCache() {
            @Override
            public Optional<Map<String, Integer>> find(UUID id, long revision) {
                return Optional.ofNullable(values.get(revision));
            }

            @Override
            public void put(UUID id, long revision, Map<String, Integer> quotas) {
                values.put(revision, quotas);
            }
        };
        when(repository.findActiveQuotaRevision(organization)).thenReturn(Optional.of(1L), Optional.of(2L));
        when(repository.findActiveQuotaSnapshot(organization)).thenReturn(Optional.of(
                new OrganizationRepository.QuotaSnapshot("FREE", 1,
                        "{\"events_per_minute\":600}")), Optional.of(
                new OrganizationRepository.QuotaSnapshot("FREE", 2,
                        "{\"events_per_minute\":1200}")));
        QuotaConfigurationService service = new QuotaConfigurationService(repository, cache,
                new ObjectMapper(), new TransactionTemplate());
        assertThat(service.forOrganization(organization).get("events_per_minute")).isEqualTo(600);
        assertThat(service.forOrganization(organization).get("events_per_minute")).isEqualTo(1200);
        assertThat(values).containsKeys(1L, 2L);
    }

    @Test
    void usesPostgresWhenRedisFailsAndFailsClosedWhenPostgresFails() {
        UUID organization = UUID.randomUUID();
        OrganizationRepository repository = mock(OrganizationRepository.class);
        QuotaCache unavailable = new QuotaCache() {
            @Override
            public Optional<Map<String, Integer>> find(UUID id, long revision) {
                throw new DataAccessResourceFailureException("Redis offline");
            }

            @Override
            public void put(UUID id, long revision, Map<String, Integer> quotas) {
                throw new DataAccessResourceFailureException("Redis offline");
            }
        };
        when(repository.findActiveQuotaRevision(organization)).thenReturn(Optional.of(1L));
        when(repository.findActiveQuotaSnapshot(organization)).thenReturn(Optional.of(
                new OrganizationRepository.QuotaSnapshot("FREE", 1, "{}")));
        QuotaConfigurationService service = new QuotaConfigurationService(repository, unavailable,
                new ObjectMapper(), new TransactionTemplate());
        assertThat(service.forOrganization(organization).get("burst_events")).isEqualTo(20);
        when(repository.findActiveQuotaRevision(organization))
                .thenThrow(new DataAccessResourceFailureException("PostgreSQL offline"));
        assertThatThrownBy(() -> service.forOrganization(organization))
                .isInstanceOfSatisfying(ResponseStatusException.class, failure ->
                        assertThat(failure.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE));
    }
}

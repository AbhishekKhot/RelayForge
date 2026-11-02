package com.relayforge.ingestion.event;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

class RoutingEventLookupServiceTest {
    private static final UUID ORGANIZATION = UUID.fromString("91cd5a24-2fbb-4f40-950d-bd8dd7ec9951");
    private static final UUID EVENT = UUID.fromString("e49ad81d-2358-45fd-aac0-6b3af1c22aca");
    private static final LocalDate DAY = LocalDate.parse("2026-11-02");
    private final IngestionRepository repository = mock(IngestionRepository.class);
    private final RoutingEventLookupService service =
            new RoutingEventLookupService(repository, "ing-a", "dlv-a");

    @Test
    void returnsOnlyAuthoritativeTenantEventForMatchingLocator() {
        String locator = EventLocator.encode(EVENT, DAY, 3410, 1, "ing-a", "dlv-a");
        Instant acceptedAt = Instant.parse("2026-11-02T10:00:00Z");
        byte[] hash = new byte[] { 1, 2, 3 };
        when(repository.findRoutingEvent(ORGANIZATION, DAY, EVENT, locator))
                .thenReturn(Optional.of(new IngestionRepository.RoutingEvent("order.created", acceptedAt, hash)));

        RoutingEventLookupService.VerifiedEvent found = service.find(ORGANIZATION, locator);

        assertThat(found.eventId()).isEqualTo(EVENT);
        assertThat(found.acceptedDay()).isEqualTo(DAY);
        assertThat(found.eventType()).isEqualTo("order.created");
        assertThat(found.acceptedAt()).isEqualTo(acceptedAt);
        assertThat(found.payloadHash()).isEqualTo("010203");
        assertThat(found.organizationId()).isEqualTo(ORGANIZATION);
    }

    @Test
    void rejectsWrongTenantAndWrongShardWithoutRevealingEvent() {
        String locator = EventLocator.encode(EVENT, DAY, 3410, 1, "ing-a", "dlv-a");
        assertThatThrownBy(() -> service.find(UUID.randomUUID(), locator))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        failure -> assertThat(failure.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND));
        String wrongShard = EventLocator.encode(EVENT, DAY, 3410, 1, "other", "dlv-a");
        assertThatThrownBy(() -> service.find(ORGANIZATION, wrongShard))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        failure -> assertThat(failure.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND));
    }
}

package com.relayforge.ingestion.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class EventQueryServiceTest {
    @Test
    void rejectsMalformedAndOtherShardLocatorsBeforeStorageAccess() {
        IngestionRepository repository = mock(IngestionRepository.class);
        DeliverySummaryClient summaries = mock(DeliverySummaryClient.class);
        EventQueryService service = new EventQueryService(repository, summaries, new ObjectMapper(),
                "ing-a", "dlv-a");
        UUID organization = UUID.randomUUID();
        assertNotFound(() -> service.find(organization, "evt_invalid"));
        String otherShard = EventLocator.encode(UUID.randomUUID(), LocalDate.now(), 1, 1,
                "ing-other", "dlv-a");
        assertNotFound(() -> service.find(organization, otherShard));
        verifyNoInteractions(repository, summaries);
    }

    @Test
    void queriesUsingOrganizationDayAndEventId() {
        IngestionRepository repository = mock(IngestionRepository.class);
        DeliverySummaryClient summaries = mock(DeliverySummaryClient.class);
        EventQueryService service = new EventQueryService(repository, summaries, new ObjectMapper(),
                "ing-a", "dlv-a");
        UUID organization = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        LocalDate day = LocalDate.now();
        String locator = EventLocator.encode(eventId, day, 1, 1, "ing-a", "dlv-a");
        when(repository.findEvent(organization, day, eventId)).thenReturn(Optional.of(
                new IngestionRepository.StoredEvent("payment.completed", 1, "{\"amount\":1}",
                        null, Instant.parse("2026-10-30T00:00:00Z"))));
        when(summaries.read(organization, day, eventId))
                .thenReturn(new DeliverySummary("PENDING", 0, 0, 0, 0));
        assertThat(service.find(organization, locator).payload().path("amount").asInt()).isEqualTo(1);
        assertNotFound(() -> service.find(UUID.randomUUID(), locator));
    }

    private void assertNotFound(Runnable action) {
        assertThatThrownBy(action::run).isInstanceOf(ResponseStatusException.class)
                .satisfies(failure -> assertThat(((ResponseStatusException) failure).getStatusCode())
                        .isEqualTo(HttpStatus.NOT_FOUND));
    }
}

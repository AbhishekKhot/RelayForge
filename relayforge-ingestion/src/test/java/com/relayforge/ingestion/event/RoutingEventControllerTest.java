package com.relayforge.ingestion.event;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

class RoutingEventControllerTest {
    private static final String CREDENTIAL = "0123456789abcdef0123456789abcdef";
    private final RoutingEventLookupService service = mock(RoutingEventLookupService.class);
    private final RoutingEventController controller = new RoutingEventController(service, CREDENTIAL);

    @Test
    void requiresDedicatedInternalCredential() {
        UUID organization = UUID.randomUUID();
        assertThatThrownBy(() -> controller.find("evt_locator", organization, null))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        failure -> assertThat(failure.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED));
        assertThatThrownBy(() -> controller.find("evt_locator", organization, "wrong"))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        failure -> assertThat(failure.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED));
    }

    @Test
    void returnsOnlyVerifiedReferenceMetadata() {
        UUID organization = UUID.randomUUID();
        UUID event = UUID.randomUUID();
        RoutingEventLookupService.VerifiedEvent verified =
                new RoutingEventLookupService.VerifiedEvent(organization, event,
                        LocalDate.parse("2026-11-02"), Instant.parse("2026-11-02T10:00:00Z"),
                        "order.created", "aabbcc");
        when(service.find(organization, "evt_locator")).thenReturn(verified);
        assertThat(controller.find("evt_locator", organization, CREDENTIAL)).isEqualTo(verified);
    }
}

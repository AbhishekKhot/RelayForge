package com.relayforge.deliveryapi.history;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;

class DeliveryQueryParametersTest {
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-11-02T10:00:00Z"), ZoneOffset.UTC);
    private static final UUID ORGANIZATION = UUID.randomUUID();

    @Test
    void roundTripsCursorWithFiltersAndStableSortKey() {
        var page = DeliveryQueryParameters.parse(ORGANIZATION, "SUCCEEDED", UUID.randomUUID().toString(),
                "2026-10-01T00:00:00Z", "2026-11-02T10:00:00Z", "25", null, CLOCK);
        String cursor = DeliveryQueryParameters.cursor(page,
                Instant.parse("2026-10-20T12:00:00Z"), UUID.randomUUID());
        var continuation = DeliveryQueryParameters.parse(ORGANIZATION, "SUCCEEDED",
                page.endpointId().toString(), "2026-10-01T00:00:00Z", "2026-11-02T10:00:00Z",
                "25", cursor, CLOCK);
        assertThat(continuation.cursor()).isNotNull();
        assertThat(continuation.cursor().createdAt()).isEqualTo(Instant.parse("2026-10-20T12:00:00Z"));
        assertThat(continuation.limit()).isEqualTo(25);
        var cursorOnly = DeliveryQueryParameters.parse(ORGANIZATION, "SUCCEEDED",
                page.endpointId().toString(), null, null, "25", cursor, CLOCK);
        assertThat(cursorOnly.from()).isEqualTo(page.from());
        assertThat(cursorOnly.to()).isEqualTo(page.to());
        assertThatThrownBy(() -> DeliveryQueryParameters.parse(ORGANIZATION, "PENDING",
                page.endpointId().toString(), "2026-10-01T00:00:00Z", "2026-11-02T10:00:00Z",
                "25", cursor, CLOCK)).isInstanceOf(ResponseStatusException.class);
        assertThatThrownBy(() -> DeliveryQueryParameters.parse(UUID.randomUUID(), "SUCCEEDED",
                page.endpointId().toString(), null, null, "25", cursor, CLOCK))
                .isInstanceOf(ResponseStatusException.class);
    }

    @Test
    void rejectsUnboundedOrMalformedSearch() {
        assertThatThrownBy(() -> DeliveryQueryParameters.parse(ORGANIZATION, null, null,
                "2026-01-01T00:00:00Z", null, null, null, CLOCK))
                .isInstanceOf(ResponseStatusException.class);
        assertThatThrownBy(() -> DeliveryQueryParameters.parse(ORGANIZATION, "INVALID", null,
                null, null, null, null, CLOCK)).isInstanceOf(ResponseStatusException.class);
        assertThatThrownBy(() -> DeliveryQueryParameters.parse(ORGANIZATION, null, null,
                null, null, "1000", null, CLOCK)).isInstanceOf(ResponseStatusException.class);
        assertThatThrownBy(() -> DeliveryQueryParameters.parse(ORGANIZATION, null, null,
                null, null, null, "not-base64", CLOCK)).isInstanceOf(ResponseStatusException.class);
    }
}

package com.relayforge.ingestion.event;

import java.time.LocalDate;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EventLocatorTest {
    @Test
    void carriesVersionedRoutingFields() {
        UUID id = UUID.fromString("00000000-0000-0000-0000-000000000001");
        String external = EventLocator.encode(id, LocalDate.of(2026, 10, 30), 3410, 1,
                "ing-a", "dlv-a");
        EventLocator.Decoded decoded = EventLocator.decode(external);
        assertThat(decoded.version()).isEqualTo(1);
        assertThat(decoded.eventId()).isEqualTo(id);
        assertThat(decoded.acceptedDay()).isEqualTo(LocalDate.of(2026, 10, 30));
        assertThat(decoded.bucketId()).isEqualTo(3410);
        assertThat(decoded.mapEpoch()).isEqualTo(1);
        assertThat(decoded.ingestionShardId()).isEqualTo("ing-a");
        assertThat(decoded.deliveryShardId()).isEqualTo("dlv-a");
    }
}

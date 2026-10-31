package com.relayforge.deliveryapi.summary;

import java.time.LocalDate;
import java.util.Optional;
import java.util.UUID;

public interface DeliverySummaryRepository {
    Optional<DeliverySummary> find(UUID organizationId, LocalDate acceptedDay, UUID eventId);
}

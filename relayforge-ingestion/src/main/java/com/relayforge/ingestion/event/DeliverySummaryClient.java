package com.relayforge.ingestion.event;

import java.time.LocalDate;
import java.util.UUID;

public interface DeliverySummaryClient {
    DeliverySummary read(UUID organizationId, LocalDate acceptedDay, UUID eventId);
}

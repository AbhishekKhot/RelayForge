package com.relayforge.deliveryapi.history;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface DeliveryHistoryRepository {
    List<DeliveryRow> list(UUID organizationId, LocalDate retentionDay,
            DeliveryQueryParameters.Page page);

    Optional<DeliveryRow> find(UUID organizationId, UUID deliveryId, LocalDate retentionDay);

    List<AttemptRow> attempts(UUID organizationId, LocalDate acceptedDay, UUID deliveryId);

    record DeliveryRow(LocalDate acceptedDay, UUID deliveryId, String eventLocator,
            UUID endpointId, UUID subscriptionId, int generation, String status,
            int attemptCount, int claimCount, Instant nextAttemptAt,
            Integer lastHttpStatus, String lastErrorCode, Instant createdAt,
            Instant updatedAt, Instant completedAt) {
    }

    record AttemptRow(int attemptNumber, Instant startedAt, Instant finishedAt,
            String outcome, Integer httpStatus, Integer durationMs,
            String errorCode, String responseExcerpt) {
    }
}

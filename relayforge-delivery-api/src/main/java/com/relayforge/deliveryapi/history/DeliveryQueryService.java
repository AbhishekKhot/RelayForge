package com.relayforge.deliveryapi.history;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public final class DeliveryQueryService {
    private final DeliveryHistoryRepository repository;
    private final Clock clock;

    public DeliveryQueryService(DeliveryHistoryRepository repository, Clock clock) {
        this.repository = Objects.requireNonNull(repository);
        this.clock = Objects.requireNonNull(clock);
    }

    public PageResult list(UUID organizationId, String status, String endpointId,
            String from, String to, String limit, String cursor) {
        DeliveryQueryParameters.Page page = DeliveryQueryParameters.parse(organizationId,
                status, endpointId, from, to, limit, cursor, clock);
        List<DeliveryHistoryRepository.DeliveryRow> found;
        try {
            found = repository.list(organizationId, retentionDay(), page);
        } catch (DataAccessException unavailable) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
        }
        boolean more = found.size() > page.limit();
        List<DeliveryHistoryRepository.DeliveryRow> selected = more
                ? found.subList(0, page.limit()) : found;
        String next = more ? DeliveryQueryParameters.cursor(page,
                selected.getLast().createdAt(), selected.getLast().deliveryId()) : null;
        return new PageResult(selected.stream().map(DeliveryQueryService::view).toList(), next);
    }

    public Optional<DetailView> detail(UUID organizationId, UUID deliveryId) {
        if (organizationId == null || deliveryId == null) {
            throw new IllegalArgumentException("Invalid delivery lookup");
        }
        try {
            return repository.find(organizationId, deliveryId, retentionDay()).map(row ->
                    new DetailView(view(row), repository.attempts(organizationId,
                            row.acceptedDay(), deliveryId).stream()
                            .map(DeliveryQueryService::attempt).toList()));
        } catch (DataAccessException unavailable) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
        }
    }

    private LocalDate retentionDay() {
        return clock.instant().atZone(ZoneOffset.UTC).toLocalDate().minusDays(90);
    }

    private static DeliveryView view(DeliveryHistoryRepository.DeliveryRow row) {
        return new DeliveryView(row.deliveryId(), row.eventLocator(), row.endpointId(),
                row.subscriptionId(), row.generation(), row.status(), row.attemptCount(),
                row.claimCount(), row.nextAttemptAt(), row.lastHttpStatus(), row.lastErrorCode(),
                row.createdAt(), row.updatedAt(), row.completedAt());
    }

    private static AttemptView attempt(DeliveryHistoryRepository.AttemptRow row) {
        return new AttemptView(row.attemptNumber(), row.startedAt(), row.finishedAt(),
                row.outcome(), row.httpStatus(), row.durationMs(), row.errorCode(),
                row.responseExcerpt());
    }

    public record PageResult(List<DeliveryView> data, String nextCursor) {
    }

    public record DetailView(DeliveryView delivery, List<AttemptView> attempts) {
    }

    public record DeliveryView(@JsonProperty("delivery_id") UUID deliveryId,
            @JsonProperty("event_id") String eventId,
            @JsonProperty("endpoint_id") UUID endpointId,
            @JsonProperty("subscription_id") UUID subscriptionId,
            int generation, String status,
            @JsonProperty("attempt_count") int attemptCount,
            @JsonProperty("claim_count") int claimCount,
            @JsonProperty("next_attempt_at") Instant nextAttemptAt,
            @JsonProperty("last_http_status") Integer lastHttpStatus,
            @JsonProperty("last_error_code") String lastErrorCode,
            @JsonProperty("created_at") Instant createdAt,
            @JsonProperty("updated_at") Instant updatedAt,
            @JsonProperty("completed_at") Instant completedAt) {
    }

    public record AttemptView(@JsonProperty("attempt_number") int attemptNumber,
            @JsonProperty("started_at") Instant startedAt,
            @JsonProperty("finished_at") Instant finishedAt,
            String outcome, @JsonProperty("http_status") Integer httpStatus,
            @JsonProperty("duration_ms") Integer durationMs,
            @JsonProperty("error_code") String errorCode,
            @JsonProperty("response_excerpt") String responseExcerpt) {
    }
}

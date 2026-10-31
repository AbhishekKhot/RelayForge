package com.relayforge.outbox;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

public interface OutboxRepository {
    List<ClaimedOutboxRow> claim(String workerId, int batchSize, Duration lease);

    boolean markPublished(UUID id, UUID claimToken);

    boolean retry(UUID id, UUID claimToken, Duration delay, String errorCode);
}

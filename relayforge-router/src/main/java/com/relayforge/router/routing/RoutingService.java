package com.relayforge.router.routing;

import java.util.Objects;
import org.springframework.transaction.support.TransactionTemplate;

public final class RoutingService {
    private final RoutingRepository repository;
    private final TransactionTemplate transactions;
    private final int chunkSize;
    private final int maxTargets;

    public RoutingService(RoutingRepository repository, TransactionTemplate transactions,
            int chunkSize, int maxTargets) {
        this.repository = Objects.requireNonNull(repository);
        this.transactions = Objects.requireNonNull(transactions);
        if (chunkSize < 1 || chunkSize > 1000 || maxTargets < chunkSize || maxTargets > 100000) {
            throw new IllegalArgumentException("Invalid routing bounds");
        }
        this.chunkSize = chunkSize;
        this.maxTargets = maxTargets;
    }

    public void route(VerifiedEvent event) {
        Objects.requireNonNull(event);
        transactions.executeWithoutResult(status -> repository.initialize(event, maxTargets));
        int completed;
        do {
            completed = transactions.execute(status -> repository.materializeChunk(event, chunkSize));
        } while (completed > 0);
    }
}

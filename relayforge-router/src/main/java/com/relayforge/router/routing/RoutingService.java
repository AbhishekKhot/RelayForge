package com.relayforge.router.routing;

import java.util.Objects;
import java.util.function.IntConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionTemplate;

public final class RoutingService {
    private static final Logger LOG = LoggerFactory.getLogger(RoutingService.class);
    private final RoutingRepository repository;
    private final TransactionTemplate transactions;
    private final int chunkSize;
    private final int maxTargets;
    private final IntConsumer fanout;

    public RoutingService(RoutingRepository repository, TransactionTemplate transactions,
            int chunkSize, int maxTargets) {
        this(repository, transactions, chunkSize, maxTargets, ignored -> { });
    }

    public RoutingService(RoutingRepository repository, TransactionTemplate transactions,
            int chunkSize, int maxTargets, IntConsumer fanout) {
        this.repository = Objects.requireNonNull(repository);
        this.transactions = Objects.requireNonNull(transactions);
        this.fanout = Objects.requireNonNull(fanout);
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
        int targetCount = 0;
        do {
            completed = transactions.execute(status -> repository.materializeChunk(event, chunkSize));
            targetCount += completed;
        } while (completed > 0);
        fanout.accept(targetCount);
        LOG.info("Event routed organizationId={} eventId={} targets={}",
                event.organizationId(), event.eventId(), targetCount);
    }
}

package com.relayforge.ingestion.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "relayforge.database.username")
public final class RateCounterCleanup {
    private static final Logger LOGGER = LoggerFactory.getLogger(RateCounterCleanup.class);
    private final IngestionRepository repository;

    public RateCounterCleanup(IngestionRepository repository) {
        this.repository = repository;
    }

    @Scheduled(fixedDelayString = "${relayforge.ingestion.rate-cleanup-delay-ms:60000}")
    public void deleteExpired() {
        try {
            repository.deleteExpiredRateCounters(1000);
        } catch (DataAccessException unavailable) {
            LOGGER.warn("Ingestion rate counter cleanup unavailable");
        }
    }
}

package com.relayforge.ingestion.event;

public record DeliverySummary(String status, int targetCount, int deliveryCount,
        int succeededCount, int deadLetteredCount) {
}

package com.relayforge.deliveryapi.summary;

import com.fasterxml.jackson.annotation.JsonProperty;

public record DeliverySummary(String status, @JsonProperty("target_count") int targetCount,
        @JsonProperty("delivery_count") int deliveryCount,
        @JsonProperty("succeeded_count") int succeededCount,
        @JsonProperty("dead_lettered_count") int deadLetteredCount) {
    public static DeliverySummary pending() {
        return new DeliverySummary("PENDING", 0, 0, 0, 0);
    }
}

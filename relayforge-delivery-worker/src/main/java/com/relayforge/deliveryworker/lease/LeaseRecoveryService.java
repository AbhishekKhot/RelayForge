package com.relayforge.deliveryworker.lease;

import java.util.Objects;
import org.springframework.scheduling.annotation.Scheduled;

public final class LeaseRecoveryService {
    private final DeliveryClaimService claims;

    public LeaseRecoveryService(DeliveryClaimService claims) {
        this.claims = Objects.requireNonNull(claims);
    }

    @Scheduled(fixedDelay = 1000)
    public void recover() {
        claims.recoverExpired(100);
    }
}

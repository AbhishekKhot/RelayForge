package com.relayforge.deliveryworker.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.relayforge.deliveryworker.lease.DeliveryClaimService;
import java.util.function.BooleanSupplier;

public interface AuthenticationStrategy {
    AuthMaterial resolve(JsonNode configuration, DeliveryClaimService.Lease lease,
            BooleanSupplier leaseValid);
}

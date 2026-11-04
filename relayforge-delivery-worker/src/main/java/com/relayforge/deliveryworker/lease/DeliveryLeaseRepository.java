package com.relayforge.deliveryworker.lease;

import java.util.Optional;
import java.util.UUID;

public interface DeliveryLeaseRepository {
    Optional<DeliveryClaimService.Lease> claim(UUID deliveryId, String workerId,
            int endpointLimit, int organizationLimit, int platformLimit);

    Optional<DeliveryClaimService.Lease> renew(DeliveryClaimService.Lease lease);

    boolean owns(DeliveryClaimService.Lease lease);

    int recoverExpired(int limit);
}

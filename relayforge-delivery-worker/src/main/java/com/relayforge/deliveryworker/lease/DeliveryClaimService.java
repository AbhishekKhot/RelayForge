package com.relayforge.deliveryworker.lease;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.springframework.transaction.support.TransactionTemplate;

public final class DeliveryClaimService {
    private final DeliveryLeaseRepository repository;
    private final TransactionTemplate transactions;

    public DeliveryClaimService(DeliveryLeaseRepository repository, TransactionTemplate transactions) {
        this.repository = Objects.requireNonNull(repository);
        this.transactions = Objects.requireNonNull(transactions);
    }

    public Optional<Lease> claim(UUID deliveryId, String workerId, int endpointLimit, int platformLimit) {
        return claim(deliveryId, workerId, endpointLimit, 20000, platformLimit);
    }

    public Optional<Lease> claim(UUID deliveryId, String workerId, int endpointLimit,
            int organizationLimit, int platformLimit) {
        if (deliveryId == null || workerId == null
                || !workerId.matches("[A-Za-z0-9][A-Za-z0-9._:-]{0,159}")
                || endpointLimit < 1 || endpointLimit > 1000
                || organizationLimit < 1 || organizationLimit > 20000
                || platformLimit < 1 || platformLimit > 10000) {
            throw new IllegalArgumentException("Invalid delivery claim");
        }
        return transactions.execute(status -> repository.claim(deliveryId, workerId,
                endpointLimit, organizationLimit, platformLimit));
    }

    public Optional<Lease> renew(Lease lease) {
        Objects.requireNonNull(lease);
        return transactions.execute(status -> repository.renew(lease));
    }

    public boolean owns(Lease lease) {
        Objects.requireNonNull(lease);
        return repository.owns(lease);
    }

    public int recoverExpired(int limit) {
        if (limit < 1 || limit > 100) {
            throw new IllegalArgumentException("Invalid recovery batch");
        }
        return transactions.execute(status -> repository.recoverExpired(limit));
    }

    public record Lease(UUID deliveryId, LocalDate acceptedDay, UUID organizationId, UUID endpointId,
            String workerId, UUID token, long version, Instant claimedAt, Instant expiresAt,
            Instant deadline) {
        public Lease {
            Objects.requireNonNull(deliveryId);
            Objects.requireNonNull(acceptedDay);
            Objects.requireNonNull(organizationId);
            Objects.requireNonNull(endpointId);
            Objects.requireNonNull(workerId);
            Objects.requireNonNull(token);
            Objects.requireNonNull(claimedAt);
            Objects.requireNonNull(expiresAt);
            Objects.requireNonNull(deadline);
            if (version < 1 || !claimedAt.isBefore(expiresAt) || expiresAt.isAfter(deadline)) {
                throw new IllegalArgumentException("Invalid delivery lease");
            }
        }
    }
}

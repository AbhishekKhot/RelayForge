package com.relayforge.deliveryworker.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.relayforge.deliveryworker.lease.DeliveryClaimService;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.function.DoubleSupplier;
import java.util.function.Supplier;

public final class TokenCacheService {
    private static final Duration LOCK_TTL = Duration.ofSeconds(100);
    private static final Duration LOCK_WAIT = Duration.ofSeconds(1);
    private final TokenExchangeService exchange;
    private final TokenCacheStore store;
    private final Clock clock;
    private final DoubleSupplier random;
    private final Supplier<UUID> lockOwner;
    private final TokenExchangeService.Sleeper sleeper;
    private final ConcurrentHashMap<TokenCacheKey, CachedToken> local = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<TokenCacheKey, CompletableFuture<AuthMaterial>> flights =
            new ConcurrentHashMap<>();

    public TokenCacheService(TokenExchangeService exchange, TokenCacheStore store, Clock clock,
            DoubleSupplier random, Supplier<UUID> lockOwner, TokenExchangeService.Sleeper sleeper) {
        this.exchange = Objects.requireNonNull(exchange);
        this.store = Objects.requireNonNull(store);
        this.clock = Objects.requireNonNull(clock);
        this.random = Objects.requireNonNull(random);
        this.lockOwner = Objects.requireNonNull(lockOwner);
        this.sleeper = Objects.requireNonNull(sleeper);
    }

    public AuthMaterial resolve(JsonNode configuration, DeliveryClaimService.Lease lease,
            BooleanSupplier leaseValid, TokenCacheKey key) {
        return singleFlight(configuration, lease, leaseValid, key);
    }

    public AuthMaterial refresh(JsonNode configuration, DeliveryClaimService.Lease lease,
            BooleanSupplier leaseValid, TokenCacheKey key, AuthMaterial previous) {
        String authorization = previous.headers().get("Authorization");
        if (authorization == null || !authorization.startsWith("Bearer ")) {
            throw new IllegalArgumentException("Invalid prior token");
        }
        String stale = authorization.substring(7);
        local.computeIfPresent(key, (ignored, value) -> value.token().equals(stale) ? null : value);
        try {
            store.invalidateIfToken(key, stale);
        } catch (RuntimeException unavailable) {
            // Local single-flight remains active when Redis is unavailable.
        }
        return singleFlight(configuration, lease, leaseValid, key);
    }

    private AuthMaterial singleFlight(JsonNode configuration, DeliveryClaimService.Lease lease,
            BooleanSupplier leaseValid, TokenCacheKey key) {
        Objects.requireNonNull(key);
        CachedToken cached = local.get(key);
        if (cached != null && cached.usable(key, clock.instant())) {
            return cached.material();
        }
        CompletableFuture<AuthMaterial> mine = new CompletableFuture<>();
        CompletableFuture<AuthMaterial> running = flights.putIfAbsent(key, mine);
        if (running != null) {
            return await(running, leaseValid);
        }
        try {
            AuthMaterial resolved = acquire(configuration, lease, leaseValid, key);
            mine.complete(resolved);
            return resolved;
        } catch (RuntimeException failed) {
            mine.completeExceptionally(failed);
            throw failed;
        } finally {
            flights.remove(key, mine);
        }
    }

    private AuthMaterial await(CompletableFuture<AuthMaterial> running, BooleanSupplier leaseValid) {
        long deadline = System.nanoTime() + Duration.ofSeconds(95).toNanos();
        while (leaseValid.getAsBoolean() && System.nanoTime() < deadline) {
            try {
                return running.get(250, TimeUnit.MILLISECONDS);
            } catch (TimeoutException waiting) {
                // Check the lease and deadline before waiting again.
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new TokenExchangeService.TokenFailure(true, "AUTH_INTERRUPTED");
            } catch (ExecutionException failed) {
                if (failed.getCause() instanceof RuntimeException runtime) {
                    throw runtime;
                }
                throw new TokenExchangeService.TokenFailure(true, "AUTH_CACHE_UNAVAILABLE");
            }
        }
        throw new TokenExchangeService.TokenFailure(true, "AUTH_CACHE_BUSY");
    }

    private AuthMaterial acquire(JsonNode configuration, DeliveryClaimService.Lease lease,
            BooleanSupplier leaseValid, TokenCacheKey key) {
        String owner = lockOwner.get().toString();
        boolean locked;
        try {
            Optional<CachedToken> cached = store.get(key);
            if (cached.isPresent() && cached.get().usable(key, clock.instant())) {
                remember(key, cached.get());
                return cached.get().material();
            }
            locked = store.acquireLock(key, owner, LOCK_TTL);
        } catch (RuntimeException unavailable) {
            return fetch(configuration, lease, leaseValid, key, false);
        }
        if (!locked) {
            return waitForOtherWorker(configuration, lease, key, leaseValid);
        }
        try {
            try {
                Optional<CachedToken> cached = store.get(key);
                if (cached.isPresent() && cached.get().usable(key, clock.instant())) {
                    remember(key, cached.get());
                    return cached.get().material();
                }
            } catch (RuntimeException unavailable) {
                // Keep the acquired lock and fetch directly.
            }
            return fetch(configuration, lease, leaseValid, key, true);
        } finally {
            try {
                store.releaseLock(key, owner);
            } catch (RuntimeException unavailable) {
                // Lock expiry bounds cleanup if Redis is unavailable.
            }
        }
    }

    private AuthMaterial waitForOtherWorker(JsonNode configuration, DeliveryClaimService.Lease lease,
            TokenCacheKey key, BooleanSupplier leaseValid) {
        long deadline = System.nanoTime() + LOCK_WAIT.toNanos();
        while (System.nanoTime() < deadline && leaseValid.getAsBoolean()) {
            try {
                Optional<CachedToken> cached = store.get(key);
                if (cached.isPresent() && cached.get().usable(key, clock.instant())) {
                    remember(key, cached.get());
                    return cached.get().material();
                }
                sleeper.sleep(Duration.ofMillis(50));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new TokenExchangeService.TokenFailure(true, "AUTH_INTERRUPTED");
            } catch (RuntimeException unavailable) {
                return fetch(configuration, lease, leaseValid, key, false);
            }
        }
        throw new TokenExchangeService.TokenFailure(true, "AUTH_CACHE_BUSY");
    }

    private AuthMaterial fetch(JsonNode configuration, DeliveryClaimService.Lease lease,
            BooleanSupplier leaseValid, TokenCacheKey key, boolean publish) {
        TokenExchangeService.AcquiredToken acquired = exchange.acquireToken(configuration, lease, leaseValid);
        CachedToken cached = cacheable(key, acquired);
        if (cached != null) {
            remember(key, cached);
            if (publish) {
                try {
                    store.put(key, cached, Duration.between(clock.instant(), cached.cacheUntil()));
                } catch (RuntimeException unavailable) {
                    // The token remains usable in this worker during a Redis outage.
                }
            }
        }
        return acquired.material();
    }

    private CachedToken cacheable(TokenCacheKey key, TokenExchangeService.AcquiredToken acquired) {
        if (acquired.expiresAt() == null) {
            return null;
        }
        Instant now = clock.instant();
        long remaining = Duration.between(now, acquired.expiresAt()).toMillis();
        if (remaining < 10000) {
            return null;
        }
        long safety = Math.min(15000, Math.max(1000, remaining / 10));
        long base = Math.min(3600000, remaining - safety);
        double draw = random.getAsDouble();
        if (!Double.isFinite(draw) || draw < 0 || draw >= 1) {
            throw new IllegalStateException("Invalid token cache randomness");
        }
        long jitter = (long) (draw * Math.min(10000, base / 10));
        long ttl = base - jitter;
        if (ttl < 5000) {
            return null;
        }
        return new CachedToken(acquired.token(), "Bearer", acquired.expiresAt(),
                now.plusMillis(ttl), key.authConfigVersion(), key.credentialVersion());
    }

    private void remember(TokenCacheKey key, CachedToken token) {
        local.put(key, token);
        if (local.size() > 10000) {
            Instant now = clock.instant();
            local.entrySet().removeIf(entry -> !entry.getValue().usable(entry.getKey(), now));
            if (local.size() > 10000) {
                local.clear();
            }
        }
    }
}

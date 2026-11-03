package com.relayforge.deliveryworker.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.deliveryworker.lease.DeliveryClaimService;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

class TokenCacheServiceTest {
    private static final Instant NOW = Instant.parse("2026-11-03T10:00:00Z");
    private final TokenExchangeService exchange = mock(TokenExchangeService.class);
    private final Store store = new Store();
    private final TokenCacheService cache = new TokenCacheService(exchange, store,
            Clock.fixed(NOW, ZoneOffset.UTC), () -> 0.5, UUID::randomUUID, duration -> { });
    private final DeliveryClaimService.Lease lease = new DeliveryClaimService.Lease(UUID.randomUUID(),
            LocalDate.parse("2026-11-03"), UUID.randomUUID(), UUID.randomUUID(), "worker",
            UUID.randomUUID(), 1, NOW, NOW.plusSeconds(60), NOW.plusSeconds(240));
    private final TokenCacheKey key = new TokenCacheKey(lease.organizationId(), lease.endpointId(), 1, 1);
    private final JsonNode config = new ObjectMapper().createObjectNode();

    @Test
    void sameKeyUsesOneTokenAndVersionsSeparateEntries() {
        when(exchange.acquireToken(any(), any(), any())).thenReturn(
                new TokenExchangeService.AcquiredToken("first", NOW.plusSeconds(120)),
                new TokenExchangeService.AcquiredToken("second", NOW.plusSeconds(120)));
        assertThat(cache.resolve(config, lease, () -> true, key).headers())
                .containsEntry("Authorization", "Bearer first");
        assertThat(cache.resolve(config, lease, () -> true, key).headers())
                .containsEntry("Authorization", "Bearer first");
        assertThat(cache.resolve(config, lease, () -> true,
                new TokenCacheKey(key.organizationId(), key.endpointId(), 1, 2)).headers())
                .containsEntry("Authorization", "Bearer second");
        verify(exchange, times(2)).acquireToken(any(), any(), any());
        assertThat(store.value.cacheUntil()).isBefore(store.value.expiresAt());
    }

    @Test
    void localSingleFlightSharesConcurrentMiss() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(exchange.acquireToken(any(), any(), any())).thenAnswer(invocation -> {
            entered.countDown();
            release.await();
            return new TokenExchangeService.AcquiredToken("shared", NOW.plusSeconds(120));
        });
        try (var pool = Executors.newFixedThreadPool(2)) {
            var first = pool.submit(() -> cache.resolve(config, lease, () -> true, key));
            entered.await();
            var second = pool.submit(() -> cache.resolve(config, lease, () -> true, key));
            release.countDown();
            assertThat(first.get().headers()).isEqualTo(second.get().headers());
        }
        verify(exchange, times(1)).acquireToken(any(), any(), any());
    }

    @Test
    void refreshInvalidatesOnlyChallengedToken() {
        when(exchange.acquireToken(any(), any(), any())).thenReturn(
                new TokenExchangeService.AcquiredToken("old", NOW.plusSeconds(120)),
                new TokenExchangeService.AcquiredToken("new", NOW.plusSeconds(120)));
        AuthMaterial old = cache.resolve(config, lease, () -> true, key);
        assertThat(cache.refresh(config, lease, () -> true, key, old).headers())
                .containsEntry("Authorization", "Bearer new");
        assertThat(cache.resolve(config, lease, () -> true, key).headers())
                .containsEntry("Authorization", "Bearer new");
        verify(exchange, times(2)).acquireToken(any(), any(), any());
    }

    @Test
    void redisOutageFallsBackLocallyAndUnknownExpiryIsNotCached() {
        store.unavailable = true;
        when(exchange.acquireToken(any(), any(), any())).thenReturn(
                new TokenExchangeService.AcquiredToken("uncertain", null));
        cache.resolve(config, lease, () -> true, key);
        cache.resolve(config, lease, () -> true, key);
        verify(exchange, times(2)).acquireToken(any(), any(), any());
        assertThatThrownBy(() -> new CachedToken("secret", "Bearer", NOW, NOW, 1, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void redisOutageSharesExpiringTokenWithinWorker() {
        store.unavailable = true;
        when(exchange.acquireToken(any(), any(), any())).thenReturn(
                new TokenExchangeService.AcquiredToken("local", NOW.plusSeconds(120)));
        assertThat(cache.resolve(config, lease, () -> true, key).headers())
                .containsEntry("Authorization", "Bearer local");
        assertThat(cache.resolve(config, lease, () -> true, key).headers())
                .containsEntry("Authorization", "Bearer local");
        verify(exchange, times(1)).acquireToken(any(), any(), any());
    }

    private static final class Store implements TokenCacheStore {
        private CachedToken value;
        private boolean unavailable;

        @Override
        public Optional<CachedToken> get(TokenCacheKey key) {
            check();
            return Optional.ofNullable(value);
        }

        @Override
        public boolean acquireLock(TokenCacheKey key, String owner, Duration ttl) {
            check();
            return true;
        }

        @Override
        public void put(TokenCacheKey key, CachedToken token, Duration ttl) {
            check();
            value = token;
        }

        @Override
        public void releaseLock(TokenCacheKey key, String owner) {
            check();
        }

        @Override
        public void invalidateIfToken(TokenCacheKey key, String token) {
            check();
            if (value != null && value.token().equals(token)) {
                value = null;
            }
        }

        private void check() {
            if (unavailable) {
                throw new IllegalStateException("Redis unavailable");
            }
        }
    }
}

package com.relayforge.deliveryworker.auth;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

class RedisTokenCacheStoreIT {
    @Test
    void lockOwnershipAndConditionalInvalidationPreserveNewToken() {
        try (var redis = new GenericContainer<>(DockerImageName.parse("redis:7.4.8-alpine"))
                .withExposedPorts(6379)) {
            redis.start();
            var connection = new LettuceConnectionFactory(new RedisStandaloneConfiguration(
                    redis.getHost(), redis.getMappedPort(6379)));
            connection.afterPropertiesSet();
            try {
                var template = new StringRedisTemplate(connection);
                var store = new RedisTokenCacheStore(template, new ObjectMapper());
                var key = new TokenCacheKey(UUID.randomUUID(), UUID.randomUUID(), 2, 3);
                assertThat(store.acquireLock(key, "owner-a", Duration.ofSeconds(5))).isTrue();
                assertThat(store.acquireLock(key, "owner-b", Duration.ofSeconds(5))).isFalse();
                store.releaseLock(key, "owner-b");
                assertThat(store.acquireLock(key, "owner-b", Duration.ofSeconds(5))).isFalse();
                store.releaseLock(key, "owner-a");
                assertThat(store.acquireLock(key, "owner-b", Duration.ofSeconds(5))).isTrue();
                Instant now = Instant.now();
                var old = new CachedToken("secret-old", "Bearer", now.plusSeconds(120),
                        now.plusSeconds(90), 2, 3);
                store.put(key, old, Duration.ofSeconds(90));
                assertThat(store.get(key)).contains(old);
                var newer = new CachedToken("secret-new", "Bearer", now.plusSeconds(120),
                        now.plusSeconds(90), 2, 3);
                store.put(key, newer, Duration.ofSeconds(90));
                store.invalidateIfToken(key, "secret-old");
                assertThat(store.get(key)).contains(newer);
                store.invalidateIfToken(key, "secret-new");
                assertThat(store.get(key)).isEmpty();
                assertThat(template.opsForValue().get(key.redisKey())).isNull();
                assertThat(newer.toString()).doesNotContain("secret-new");
            } finally {
                connection.destroy();
            }
        }
    }
}

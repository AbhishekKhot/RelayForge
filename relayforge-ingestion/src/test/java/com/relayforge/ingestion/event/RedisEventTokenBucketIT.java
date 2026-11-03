package com.relayforge.ingestion.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

class RedisEventTokenBucketIT {
    @Test
    void enforcesBurstAtomicallyAndSeparatesOrganizations() {
        try (var container = new GenericContainer<>(DockerImageName.parse("redis:7.4.8-alpine"))
                .withExposedPorts(6379)) {
            container.start();
            var server = new RedisStandaloneConfiguration(container.getHost(),
                    container.getMappedPort(6379));
            var factory = new LettuceConnectionFactory(server);
            factory.afterPropertiesSet();
            try {
                RedisEventTokenBucket bucket = new RedisEventTokenBucket(new StringRedisTemplate(factory));
                UUID first = UUID.randomUUID();
                UUID second = UUID.randomUUID();
                assertThat(bucket.tryAcquire(first, 2, 2)).isTrue();
                assertThat(bucket.tryAcquire(first, 2, 2)).isTrue();
                assertThat(bucket.tryAcquire(first, 2, 2)).isFalse();
                assertThat(bucket.tryAcquire(second, 2, 2)).isTrue();
            } finally {
                factory.destroy();
            }
        }
    }
}

package com.relayforge.controlplane.organization;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

class RedisQuotaCacheIT {
    @Test
    void expiresEntriesAndSeparatesConfigurationRevisions() throws Exception {
        try (var container = new GenericContainer<>(DockerImageName.parse("redis:7.4.8-alpine"))
                .withExposedPorts(6379)) {
            container.start();
            var server = new RedisStandaloneConfiguration(container.getHost(),
                    container.getMappedPort(6379));
            var factory = new LettuceConnectionFactory(server);
            factory.afterPropertiesSet();
            try {
                RedisQuotaCache cache = new RedisQuotaCache(new StringRedisTemplate(factory),
                        new ObjectMapper(), Duration.ofSeconds(1));
                UUID organization = UUID.randomUUID();
                cache.put(organization, 1, Map.of("events_per_minute", 600));
                assertThat(cache.find(organization, 1)).contains(Map.of("events_per_minute", 600));
                assertThat(cache.find(organization, 2)).isEmpty();
                Thread.sleep(1200);
                assertThat(cache.find(organization, 1)).isEmpty();
            } finally {
                factory.destroy();
            }
        }
    }
}

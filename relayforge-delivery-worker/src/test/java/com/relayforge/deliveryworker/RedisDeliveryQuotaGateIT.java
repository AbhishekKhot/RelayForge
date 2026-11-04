package com.relayforge.deliveryworker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

class RedisDeliveryQuotaGateIT {
    @Test
    void reservesAndReleasesTenantScopedPermits() {
        try (var container = new GenericContainer<>(DockerImageName.parse("redis:7.4.8-alpine"))
                .withExposedPorts(6379)) {
            container.start();
            var server = new RedisStandaloneConfiguration(container.getHost(),
                    container.getMappedPort(6379));
            var factory = new LettuceConnectionFactory(server);
            factory.afterPropertiesSet();
            try {
                RedisDeliveryQuotaGate gate = new RedisDeliveryQuotaGate(new StringRedisTemplate(factory));
                UUID first = UUID.randomUUID();
                UUID second = UUID.randomUUID();
                assertThat(gate.reserve(first, "permit-one", 1)).isTrue();
                assertThat(gate.reserve(first, "permit-two", 1)).isFalse();
                assertThat(gate.reserve(second, "permit-three", 1)).isTrue();
                gate.renew(first, "permit-one");
                assertThat(gate.reserve(first, "permit-two", 1)).isFalse();
                gate.release(first, "permit-one");
                assertThat(gate.reserve(first, "permit-two", 1)).isTrue();
            } finally {
                factory.destroy();
            }
        }
    }
}

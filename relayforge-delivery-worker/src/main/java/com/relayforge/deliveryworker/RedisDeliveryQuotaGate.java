package com.relayforge.deliveryworker;

import java.util.Objects;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.RedisCallback;

public final class RedisDeliveryQuotaGate implements DeliveryQuotaGate {
    private final StringRedisTemplate redis;

    public RedisDeliveryQuotaGate(StringRedisTemplate redis) {
        this.redis = Objects.requireNonNull(redis);
    }

    @Override
    public boolean available() {
        try {
            return "PONG".equals(redis.execute((RedisCallback<String>) connection -> connection.ping()));
        } catch (DataAccessException unavailable) {
            return false;
        }
    }
}

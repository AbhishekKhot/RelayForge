package com.relayforge.deliveryworker;

import java.util.Objects;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import java.util.List;
import java.util.UUID;

public final class RedisDeliveryQuotaGate implements DeliveryQuotaGate {
    private static final DefaultRedisScript<Long> RESERVE = new DefaultRedisScript<>("""
            local time = redis.call('TIME')
            local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
            redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
            if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[2]) then return 0 end
            redis.call('ZADD', KEYS[1], now + 65000, ARGV[1])
            redis.call('PEXPIRE', KEYS[1], 120000)
            return 1
            """, Long.class);
    private static final DefaultRedisScript<Long> RENEW = new DefaultRedisScript<>("""
            if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then return 0 end
            local time = redis.call('TIME')
            local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
            redis.call('ZADD', KEYS[1], now + 65000, ARGV[1])
            redis.call('PEXPIRE', KEYS[1], 120000)
            return 1
            """, Long.class);
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

    @Override
    public boolean reserve(UUID organizationId, String permitId, int limit) {
        if (organizationId == null || permitId == null || limit < 1 || limit > 20000) {
            throw new IllegalArgumentException("Invalid delivery permit");
        }
        try {
            return Long.valueOf(1).equals(redis.execute(RESERVE,
                    List.of(key(organizationId)), permitId, Integer.toString(limit)));
        } catch (DataAccessException unavailable) {
            return false;
        }
    }

    @Override
    public void renew(UUID organizationId, String permitId) {
        try {
            redis.execute(RENEW, List.of(key(organizationId)), permitId);
        } catch (DataAccessException unavailable) {
            // PostgreSQL lease renewal still decides whether the claimed work can finish.
        }
    }

    @Override
    public void release(UUID organizationId, String permitId) {
        try {
            redis.opsForZSet().remove(key(organizationId), permitId);
        } catch (DataAccessException unavailable) {
            // The short permit TTL recovers a release interrupted by Redis failure.
        }
    }

    private String key(UUID organizationId) {
        return "quota:delivery:" + organizationId;
    }
}

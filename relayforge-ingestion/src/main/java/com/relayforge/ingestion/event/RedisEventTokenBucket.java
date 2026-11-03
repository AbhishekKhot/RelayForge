package com.relayforge.ingestion.event;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

public final class RedisEventTokenBucket implements EventTokenBucket {
    private static final DefaultRedisScript<Long> RESERVE = new DefaultRedisScript<>("""
            local time = redis.call('TIME')
            local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
            local rate = tonumber(ARGV[1])
            local burst = tonumber(ARGV[2])
            local stored = redis.call('HMGET', KEYS[1], 'tokens', 'at')
            local tokens = tonumber(stored[1]) or burst
            local previous = tonumber(stored[2]) or now
            tokens = math.min(burst, tokens + math.max(0, now - previous) * rate / 60000)
            local allowed = 0
            if tokens >= 1 then
                tokens = tokens - 1
                allowed = 1
            end
            redis.call('HSET', KEYS[1], 'tokens', tokens, 'at', now)
            redis.call('PEXPIRE', KEYS[1], 120000)
            return allowed
            """, Long.class);
    private final StringRedisTemplate redis;

    public RedisEventTokenBucket(StringRedisTemplate redis) {
        this.redis = Objects.requireNonNull(redis);
    }

    @Override
    public boolean tryAcquire(UUID organizationId, int eventsPerMinute, int burstEvents) {
        if (organizationId == null || eventsPerMinute < 1 || eventsPerMinute > 60000
                || burstEvents < 1 || burstEvents > 2000 || burstEvents > eventsPerMinute) {
            throw new IllegalArgumentException("Invalid event quota");
        }
        Long allowed = redis.execute(RESERVE, List.of("quota:events:" + organizationId),
                Integer.toString(eventsPerMinute), Integer.toString(burstEvents));
        if (allowed == null) {
            throw new DataAccessResourceFailureException("Event quota coordination is unavailable");
        }
        return allowed == 1L;
    }
}

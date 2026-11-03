package com.relayforge.controlplane.organization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.redis.core.StringRedisTemplate;

public final class RedisQuotaCache implements QuotaCache {
    private static final TypeReference<Map<String, Integer>> QUOTA_TYPE = new TypeReference<>() { };
    private final StringRedisTemplate redis;
    private final ObjectMapper json;
    private final Duration ttl;

    public RedisQuotaCache(StringRedisTemplate redis, ObjectMapper json) {
        this(redis, json, Duration.ofSeconds(30));
    }

    RedisQuotaCache(StringRedisTemplate redis, ObjectMapper json, Duration ttl) {
        this.redis = redis;
        this.json = json;
        this.ttl = ttl;
    }

    @Override
    public Optional<Map<String, Integer>> find(UUID organizationId, long revision) {
        String cached = redis.opsForValue().get(key(organizationId, revision));
        if (cached == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(json.readValue(cached, QUOTA_TYPE));
        } catch (JsonProcessingException malformed) {
            return Optional.empty();
        }
    }

    @Override
    public void put(UUID organizationId, long revision, Map<String, Integer> quotas) {
        try {
            redis.opsForValue().set(key(organizationId, revision), json.writeValueAsString(quotas), ttl);
        } catch (JsonProcessingException impossible) {
            throw new IllegalStateException("Could not encode quota configuration", impossible);
        }
    }

    private String key(UUID organizationId, long revision) {
        return "quota:organization:" + organizationId + ":" + revision;
    }
}

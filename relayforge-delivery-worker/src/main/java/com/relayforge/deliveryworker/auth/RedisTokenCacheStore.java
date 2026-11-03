package com.relayforge.deliveryworker.auth;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

public final class RedisTokenCacheStore implements TokenCacheStore {
    private static final DefaultRedisScript<Long> DELETE_IF_EQUAL = new DefaultRedisScript<>(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) "
                    + "else return 0 end", Long.class);
    private final StringRedisTemplate redis;
    private final ObjectMapper json;

    public RedisTokenCacheStore(StringRedisTemplate redis, ObjectMapper json) {
        this.redis = redis;
        this.json = json;
    }

    @Override
    public Optional<CachedToken> get(TokenCacheKey key) {
        String value = redis.opsForValue().get(key.redisKey());
        if (value == null) {
            return Optional.empty();
        }
        try {
            JsonNode node = json.readTree(value);
            CachedToken token = new CachedToken(node.path("token").asText(),
                    node.path("tokenType").asText(), Instant.parse(node.path("expiresAt").asText()),
                    Instant.parse(node.path("cacheUntil").asText()),
                    node.path("authConfigVersion").asLong(), node.path("credentialVersion").asLong());
            return token.usable(key, Instant.now()) ? Optional.of(token) : Optional.empty();
        } catch (JsonProcessingException | IllegalArgumentException invalid) {
            return Optional.empty();
        }
    }

    @Override
    public boolean acquireLock(TokenCacheKey key, String owner, Duration ttl) {
        return Boolean.TRUE.equals(redis.opsForValue().setIfAbsent(key.redisKey() + ":lock", owner, ttl));
    }

    @Override
    public void put(TokenCacheKey key, CachedToken token, Duration ttl) {
        if (ttl.isNegative() || ttl.isZero() || !token.usable(key, Instant.now())) {
            return;
        }
        var node = json.createObjectNode();
        node.put("token", token.token());
        node.put("tokenType", token.tokenType());
        node.put("expiresAt", token.expiresAt().toString());
        node.put("cacheUntil", token.cacheUntil().toString());
        node.put("authConfigVersion", token.authConfigVersion());
        node.put("credentialVersion", token.credentialVersion());
        redis.opsForValue().set(key.redisKey(), node.toString(), ttl);
    }

    @Override
    public void releaseLock(TokenCacheKey key, String owner) {
        redis.execute(DELETE_IF_EQUAL, java.util.List.of(key.redisKey() + ":lock"), owner);
    }

    @Override
    public void invalidateIfToken(TokenCacheKey key, String token) {
        String value = redis.opsForValue().get(key.redisKey());
        if (value == null) {
            return;
        }
        try {
            if (json.readTree(value).path("token").asText().equals(token)) {
                redis.execute(DELETE_IF_EQUAL, java.util.List.of(key.redisKey()), value);
            }
        } catch (JsonProcessingException invalid) {
            // A malformed cache entry is unusable and expires by its Redis TTL.
        }
    }
}

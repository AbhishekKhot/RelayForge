package com.relayforge.ingestion.event;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.SocketOptions;
import java.net.URI;
import java.time.Duration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

@Configuration(proxyBeanMethods = false)
public class IngestionQuotaConfiguration {
    @Bean
    @ConditionalOnProperty(name = "relayforge.redis.uri")
    LettuceConnectionFactory ingestionQuotaRedisConnectionFactory(Environment environment) {
        URI uri = URI.create(environment.getRequiredProperty("relayforge.redis.uri"));
        if (!("redis".equals(uri.getScheme()) || "rediss".equals(uri.getScheme()))
                || uri.getHost() == null || uri.getPort() < 1 || uri.getRawUserInfo() != null
                || uri.getRawQuery() != null || uri.getRawFragment() != null
                || uri.getRawPath() != null && !uri.getRawPath().isEmpty()) {
            throw new IllegalArgumentException("Invalid Redis connection URL");
        }
        RedisStandaloneConfiguration server = new RedisStandaloneConfiguration(uri.getHost(), uri.getPort());
        server.setPassword(environment.getRequiredProperty("relayforge.redis.password"));
        var options = ClientOptions.builder().socketOptions(SocketOptions.builder()
                .connectTimeout(Duration.ofMillis(300)).build()).build();
        var builder = LettuceClientConfiguration.builder().clientOptions(options)
                .commandTimeout(Duration.ofMillis(300));
        if ("rediss".equals(uri.getScheme())) {
            builder.useSsl();
        }
        return new LettuceConnectionFactory(server, builder.build());
    }

    @Bean
    @ConditionalOnProperty(name = "relayforge.redis.uri")
    StringRedisTemplate ingestionQuotaRedisTemplate(LettuceConnectionFactory ingestionQuotaRedisConnectionFactory) {
        return new StringRedisTemplate(ingestionQuotaRedisConnectionFactory);
    }

    @Bean
    @ConditionalOnProperty(name = "relayforge.redis.uri")
    EventTokenBucket redisEventTokenBucket(StringRedisTemplate ingestionQuotaRedisTemplate) {
        return new RedisEventTokenBucket(ingestionQuotaRedisTemplate);
    }

    @Bean
    @ConditionalOnMissingBean(EventTokenBucket.class)
    EventTokenBucket postgresOnlyEventTokenBucket() {
        return (organizationId, rate, burst) -> true;
    }
}

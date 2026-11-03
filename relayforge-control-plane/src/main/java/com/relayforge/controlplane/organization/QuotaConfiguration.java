package com.relayforge.controlplane.organization;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.SocketOptions;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration(proxyBeanMethods = false)
public class QuotaConfiguration {
    @Bean
    @ConditionalOnProperty(name = "relayforge.redis.uri")
    LettuceConnectionFactory quotaRedisConnectionFactory(Environment environment) {
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
    StringRedisTemplate quotaRedisTemplate(LettuceConnectionFactory quotaRedisConnectionFactory) {
        return new StringRedisTemplate(quotaRedisConnectionFactory);
    }

    @Bean
    @ConditionalOnProperty(name = "relayforge.redis.uri")
    QuotaCache redisQuotaCache(StringRedisTemplate quotaRedisTemplate, ObjectMapper json) {
        return new RedisQuotaCache(quotaRedisTemplate, json);
    }

    @Bean
    @ConditionalOnMissingBean(QuotaCache.class)
    QuotaCache noOpQuotaCache() {
        return new QuotaCache() {
            @Override
            public Optional<Map<String, Integer>> find(UUID organizationId, long revision) {
                return Optional.empty();
            }

            @Override
            public void put(UUID organizationId, long revision, Map<String, Integer> quotas) {
            }
        };
    }

    @Bean
    @ConditionalOnProperty(name = "relayforge.database.username")
    QuotaConfigurationService quotaConfigurationService(OrganizationRepository repository,
            QuotaCache cache, ObjectMapper json, TransactionTemplate controlTransactionTemplate) {
        return new QuotaConfigurationService(repository, cache, json, controlTransactionTemplate);
    }
}

package com.relayforge.deliveryworker.lease;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.apisupport.ReadinessProbe;
import com.relayforge.deliveryworker.DeliveryKafkaLifecycle;
import com.relayforge.deliveryworker.DeliveryProcessor;
import com.relayforge.deliveryworker.DeliveryOutcomeRecorder;
import com.relayforge.deliveryworker.MicrometerDeliveryOutcomeRecorder;
import com.relayforge.deliveryworker.auth.AuthenticationStrategyResolver;
import com.relayforge.deliveryworker.auth.StaticAuthenticationStrategy;
import com.relayforge.deliveryworker.auth.TokenAuthenticationStrategy;
import com.relayforge.deliveryworker.auth.TokenExchangeService;
import com.relayforge.deliveryworker.auth.TokenCacheService;
import com.relayforge.deliveryworker.auth.RedisTokenCacheStore;
import com.relayforge.deliveryworker.DeliveryQuotaGate;
import com.relayforge.deliveryworker.RedisDeliveryQuotaGate;
import com.relayforge.deliveryworker.http.DeliveryReferenceClient;
import com.relayforge.deliveryworker.http.WebhookRequestFactory;
import com.relayforge.deliveryworker.retry.RetryPolicy;
import com.relayforge.outbound.DestinationPolicy;
import com.relayforge.outbound.SafeWebhookClient;
import java.net.InetAddress;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.RedisCallback;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.SocketOptions;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "relayforge.database.username")
@EnableScheduling
public class DeliveryLeaseConfiguration {
    @Bean(destroyMethod = "close")
    HikariDataSource deliveryWorkerDataSource(Environment environment) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(environment.getRequiredProperty("relayforge.database.url"));
        config.setUsername(environment.getRequiredProperty("relayforge.database.username"));
        config.setPassword(environment.getRequiredProperty("relayforge.database.password"));
        config.setMaximumPoolSize(8);
        config.setMinimumIdle(1);
        config.setConnectionTimeout(3000);
        config.setConnectionInitSql("SET TIME ZONE 'UTC'");
        config.addDataSourceProperty("socketTimeout", "5");
        config.addDataSourceProperty("options", "-c statement_timeout=4000");
        return new HikariDataSource(config);
    }

    @Bean
    JdbcTemplate deliveryWorkerJdbcTemplate(DataSource deliveryWorkerDataSource) {
        JdbcTemplate jdbc = new JdbcTemplate(deliveryWorkerDataSource);
        jdbc.setQueryTimeout(4);
        return jdbc;
    }

    @Bean
    DeliveryLeaseRepository deliveryLeaseRepository(JdbcTemplate deliveryWorkerJdbcTemplate) {
        return new JdbcDeliveryLeaseRepository(deliveryWorkerJdbcTemplate);
    }

    @Bean
    DeliveryClaimService deliveryClaimService(DeliveryLeaseRepository repository,
            DataSource deliveryWorkerDataSource) {
        TransactionTemplate transactions = new TransactionTemplate(
                new JdbcTransactionManager(deliveryWorkerDataSource));
        transactions.setTimeout(5);
        return new DeliveryClaimService(repository, transactions);
    }

    @Bean
    LeaseRecoveryService leaseRecoveryService(DeliveryClaimService claims) {
        return new LeaseRecoveryService(claims);
    }

    @Bean
    DeliveryAttemptService deliveryAttemptService(JdbcTemplate deliveryWorkerJdbcTemplate,
            DataSource deliveryWorkerDataSource) {
        TransactionTemplate transactions = new TransactionTemplate(
                new JdbcTransactionManager(deliveryWorkerDataSource));
        transactions.setTimeout(5);
        return new DeliveryAttemptService(deliveryWorkerJdbcTemplate, transactions);
    }

    @Bean
    DeliveryReferenceClient deliveryReferenceClient(Environment environment, ObjectMapper json) {
        return new DeliveryReferenceClient(
                URI.create(environment.getRequiredProperty("relayforge.control.internal-url")),
                URI.create(environment.getRequiredProperty("relayforge.ingestion.internal-url")),
                environment.getRequiredProperty("relayforge.delivery.config-credential"),
                environment.getRequiredProperty("relayforge.delivery.payload-credential"), json);
    }

    @Bean
    DestinationPolicy deliveryDestinationPolicy(Environment environment) {
        boolean local = environment.acceptsProfiles(Profiles.of("local"));
        String simulator = environment.getProperty("relayforge.webhook.simulator-url");
        if (local && (simulator == null || simulator.isBlank())) {
            throw new IllegalStateException("Local webhook simulator URL is required");
        }
        return new DestinationPolicy(InetAddress::getAllByName, local,
                simulator == null ? null : URI.create(simulator));
    }

    @Bean(destroyMethod = "close")
    SafeWebhookClient deliveryWebhookClient(DestinationPolicy policy) {
        return new SafeWebhookClient(policy);
    }

    @Bean
    RetryPolicy deliveryRetryPolicy(Environment environment) {
        RetryPolicy.Settings settings = new RetryPolicy.Settings(
                Duration.ofSeconds(environment.getProperty(
                        "relayforge.retry.base-delay-seconds", Long.class, 1L)),
                Duration.ofSeconds(environment.getProperty(
                        "relayforge.retry.max-delay-seconds", Long.class, 900L)),
                Duration.ofSeconds(environment.getProperty(
                        "relayforge.retry.max-after-seconds", Long.class, 3600L)),
                Duration.ofSeconds(environment.getProperty(
                        "relayforge.retry.max-age-seconds", Long.class, 86400L)));
        return new RetryPolicy(Clock.systemUTC(), () -> ThreadLocalRandom.current().nextDouble(),
                settings);
    }

    @Bean
    TokenExchangeService deliveryTokenExchangeService(SafeWebhookClient client,
            DestinationPolicy policy, ObjectMapper json, DeliveryAttemptService attempts) {
        return new TokenExchangeService(client, policy, json, attempts, Clock.systemUTC(),
                () -> ThreadLocalRandom.current().nextDouble(), duration -> Thread.sleep(duration));
    }

    @Bean
    LettuceConnectionFactory deliveryTokenRedisConnectionFactory(Environment environment) {
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
    StringRedisTemplate deliveryTokenRedisTemplate(LettuceConnectionFactory factory) {
        return new StringRedisTemplate(factory);
    }

    @Bean
    ReadinessProbe deliveryRedisReadinessProbe(StringRedisTemplate deliveryTokenRedisTemplate) {
        return () -> Boolean.TRUE.equals(deliveryTokenRedisTemplate.execute(
                (RedisCallback<Boolean>) connection -> "PONG".equals(connection.ping())));
    }

    @Bean
    TokenCacheService deliveryTokenCacheService(TokenExchangeService exchange,
            StringRedisTemplate redis, ObjectMapper json, MeterRegistry registry) {
        registry.counter("relayforge.token.cache", "result", "hit");
        registry.counter("relayforge.token.cache", "result", "miss");
        return new TokenCacheService(exchange, new RedisTokenCacheStore(redis, json), Clock.systemUTC(),
                () -> ThreadLocalRandom.current().nextDouble(), java.util.UUID::randomUUID,
                duration -> Thread.sleep(duration), hit -> registry.counter("relayforge.token.cache",
                        "result", hit ? "hit" : "miss").increment());
    }

    @Bean
    AuthenticationStrategyResolver deliveryAuthenticationStrategies(TokenCacheService cache) {
        return new AuthenticationStrategyResolver(new StaticAuthenticationStrategy(),
                new TokenAuthenticationStrategy(cache));
    }

    @Bean
    DeliveryQuotaGate deliveryQuotaGate(StringRedisTemplate redis) {
        return new RedisDeliveryQuotaGate(redis);
    }

    @Bean
    DeliveryOutcomeRecorder deliveryOutcomeRecorder(MeterRegistry registry) {
        return new MicrometerDeliveryOutcomeRecorder(registry);
    }

    @Bean
    DeliveryProcessor deliveryProcessor(DeliveryClaimService claims, DeliveryAttemptService attempts,
            DeliveryReferenceClient references, DestinationPolicy policy,
            SafeWebhookClient client, RetryPolicy retryPolicy, ObjectMapper json,
            AuthenticationStrategyResolver authentication, DeliveryQuotaGate quotaGate,
            DeliveryOutcomeRecorder outcomes, Environment environment) {
        int platformLimit = environment.getProperty(
                "relayforge.delivery.platform-concurrency-limit", Integer.class, 200);
        return new DeliveryProcessor(claims, attempts, references, new WebhookRequestFactory(json),
                policy, client, retryPolicy, authentication, quotaGate, Clock.systemUTC(),
                "worker-" + java.util.UUID.randomUUID(), outcomes, platformLimit);
    }

    @Bean
    DeliveryKafkaLifecycle deliveryKafkaLifecycle(Environment environment, ObjectMapper json,
            DeliveryProcessor processor, MeterRegistry registry) {
        return new DeliveryKafkaLifecycle(
                environment.getRequiredProperty("relayforge.kafka.bootstrap-servers"), json,
                processor, registry);
    }
}

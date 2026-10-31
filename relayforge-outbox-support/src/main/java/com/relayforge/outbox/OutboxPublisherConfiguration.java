package com.relayforge.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.UUID;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.core.env.Environment;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "relayforge.outbox.owner")
@EnableScheduling
public class OutboxPublisherConfiguration {
    @Bean(destroyMethod = "close")
    HikariDataSource outboxDataSource(Environment environment) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(environment.getRequiredProperty("relayforge.database.url"));
        config.setUsername(environment.getRequiredProperty("relayforge.database.username"));
        config.setPassword(environment.getRequiredProperty("relayforge.database.password"));
        config.setMaximumPoolSize(4);
        config.setMinimumIdle(1);
        config.setConnectionTimeout(3000);
        config.setConnectionInitSql("SET TIME ZONE 'UTC'");
        config.addDataSourceProperty("socketTimeout", "5");
        config.addDataSourceProperty("options", "-c statement_timeout=4000");
        return new HikariDataSource(config);
    }

    @Bean
    JdbcTemplate outboxJdbcTemplate(DataSource outboxDataSource) {
        JdbcTemplate jdbc = new JdbcTemplate(outboxDataSource);
        jdbc.setQueryTimeout(4);
        return jdbc;
    }

    @Bean
    OutboxRepository outboxRepository(JdbcTemplate outboxJdbcTemplate, Environment environment) {
        JdbcOutboxRepository.Owner owner = JdbcOutboxRepository.Owner.valueOf(
                environment.getRequiredProperty("relayforge.outbox.owner"));
        return new JdbcOutboxRepository(outboxJdbcTemplate, owner);
    }

    @Bean(destroyMethod = "close")
    KafkaOutboxTransport outboxTransport(Environment environment) {
        return new KafkaOutboxTransport(
                environment.getRequiredProperty("relayforge.kafka.bootstrap-servers"),
                Duration.ofSeconds(10));
    }

    @Bean
    OutboxEnvelope outboxEnvelope(ObjectMapper objectMapper) {
        return new OutboxEnvelope(objectMapper);
    }

    @Bean
    OutboxPublisher outboxPublisher(OutboxRepository repository, KafkaOutboxTransport transport,
            OutboxEnvelope envelope) {
        return new OutboxPublisher(repository, transport, envelope, UUID.randomUUID().toString(),
                1, Duration.ofSeconds(30), new SecureRandom());
    }

    @Bean
    OutboxPollingJob outboxPollingJob(OutboxPublisher publisher) {
        return new OutboxPollingJob(publisher);
    }

    public static final class OutboxPollingJob {
        private static final Logger LOG = LoggerFactory.getLogger(OutboxPollingJob.class);
        private final OutboxPublisher publisher;

        public OutboxPollingJob(OutboxPublisher publisher) {
            this.publisher = publisher;
        }

        @Scheduled(fixedDelayString = "${relayforge.outbox.poll-delay-ms:250}",
                initialDelayString = "${relayforge.outbox.initial-delay-ms:1000}")
        public void poll() {
            try {
                publisher.publishBatch();
            } catch (RuntimeException unavailable) {
                LOG.warn("Outbox polling temporarily unavailable");
            }
        }
    }
}

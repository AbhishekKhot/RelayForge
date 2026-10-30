package com.relayforge.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.ingestion.event.ControlPlaneAuthenticator;
import com.relayforge.ingestion.event.DurabilityGate;
import com.relayforge.ingestion.event.EventIngestionService;
import com.relayforge.ingestion.event.EventRequestParser;
import com.relayforge.ingestion.event.IngestionRepository;
import com.relayforge.ingestion.event.JdbcIngestionRepository;
import com.relayforge.ingestion.event.OrganizationAuthenticator;
import com.relayforge.ingestion.event.PostgresDurabilityGate;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.net.URI;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "relayforge.database.username")
public class IngestionDataConfiguration {
    @Bean(destroyMethod = "close")
    HikariDataSource ingestionDataSource(Environment environment) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(environment.getRequiredProperty("relayforge.database.url"));
        config.setUsername(environment.getRequiredProperty("relayforge.database.username"));
        config.setPassword(environment.getRequiredProperty("relayforge.database.password"));
        config.setMaximumPoolSize(8);
        config.setMinimumIdle(1);
        config.setConnectionTimeout(3000);
        config.setConnectionInitSql("SET TIME ZONE 'UTC'");
        config.addDataSourceProperty("socketTimeout", "10");
        config.addDataSourceProperty("options", "-c statement_timeout=8000");
        return new HikariDataSource(config);
    }

    @Bean
    JdbcTemplate ingestionJdbcTemplate(DataSource ingestionDataSource) {
        JdbcTemplate jdbc = new JdbcTemplate(ingestionDataSource);
        jdbc.setQueryTimeout(8);
        return jdbc;
    }

    @Bean
    TransactionTemplate ingestionTransactionTemplate(DataSource ingestionDataSource) {
        TransactionTemplate transactions = new TransactionTemplate(new JdbcTransactionManager(ingestionDataSource));
        transactions.setTimeout(8);
        return transactions;
    }

    @Bean
    IngestionRepository ingestionRepository(JdbcTemplate ingestionJdbcTemplate) {
        return new JdbcIngestionRepository(ingestionJdbcTemplate);
    }

    @Bean
    EventRequestParser eventRequestParser() {
        return new EventRequestParser();
    }

    @Bean
    @ConditionalOnMissingBean(DurabilityGate.class)
    DurabilityGate ingestionDurabilityGate(JdbcTemplate ingestionJdbcTemplate, Environment environment) {
        return new PostgresDurabilityGate(ingestionJdbcTemplate,
                environment.getProperty("relayforge.ingestion.acceptance-enabled", Boolean.class, false),
                environment.getProperty("relayforge.ingestion.synchronous-standby-name"));
    }

    @Bean
    OrganizationAuthenticator organizationAuthenticator(Environment environment, ObjectMapper json) {
        return new ControlPlaneAuthenticator(
                URI.create(environment.getRequiredProperty("relayforge.control.internal-url")), json);
    }

    @Bean
    EventIngestionService eventIngestionService(IngestionRepository repository, DurabilityGate durability,
            TransactionTemplate ingestionTransactionTemplate, ObjectMapper json, Environment environment) {
        return new EventIngestionService(repository, durability, ingestionTransactionTemplate, json,
                environment.getProperty("relayforge.ingestion.map-epoch", Long.class, 1L),
                environment.getProperty("relayforge.ingestion.shard-id", "ing-a"),
                environment.getProperty("relayforge.ingestion.delivery-shard-id", "dlv-a"));
    }
}

package com.relayforge.router;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.router.routing.ConfigurationSnapshotClient;
import com.relayforge.router.routing.EventRoutingHandler;
import com.relayforge.router.routing.HttpConfigurationSnapshotClient;
import com.relayforge.router.routing.HttpEventReferenceVerifier;
import com.relayforge.router.routing.JdbcProjectionRepository;
import com.relayforge.router.routing.JdbcRoutingRepository;
import com.relayforge.router.routing.ProjectionRepository;
import com.relayforge.router.routing.ProjectionService;
import com.relayforge.router.routing.RoutingRepository;
import com.relayforge.router.routing.RoutingService;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.net.URI;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "relayforge.database.username")
public class RouterDataConfiguration {
    @Bean(destroyMethod = "close")
    HikariDataSource routerDataSource(Environment environment) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(environment.getRequiredProperty("relayforge.database.url"));
        config.setUsername(environment.getRequiredProperty("relayforge.database.username"));
        config.setPassword(environment.getRequiredProperty("relayforge.database.password"));
        config.setMaximumPoolSize(8);
        config.setMinimumIdle(1);
        config.setConnectionTimeout(3000);
        config.setConnectionInitSql("SET TIME ZONE 'UTC'");
        config.addDataSourceProperty("socketTimeout", "20");
        config.addDataSourceProperty("options", "-c statement_timeout=18000");
        return new HikariDataSource(config);
    }

    @Bean
    JdbcTemplate routerJdbcTemplate(DataSource routerDataSource) {
        JdbcTemplate jdbc = new JdbcTemplate(routerDataSource);
        jdbc.setQueryTimeout(18);
        return jdbc;
    }

    @Bean
    TransactionTemplate routerTransactionTemplate(DataSource routerDataSource) {
        TransactionTemplate transactions = new TransactionTemplate(new JdbcTransactionManager(routerDataSource));
        transactions.setTimeout(20);
        return transactions;
    }

    @Bean
    ProjectionRepository projectionRepository(JdbcTemplate routerJdbcTemplate) {
        return new JdbcProjectionRepository(routerJdbcTemplate);
    }

    @Bean
    RoutingRepository routingRepository(JdbcTemplate routerJdbcTemplate) {
        return new JdbcRoutingRepository(routerJdbcTemplate);
    }

    @Bean
    ConfigurationSnapshotClient configurationSnapshotClient(Environment environment, ObjectMapper json) {
        return new HttpConfigurationSnapshotClient(
                URI.create(environment.getRequiredProperty("relayforge.control.internal-url")),
                environment.getRequiredProperty("relayforge.router.config-credential"), json);
    }

    @Bean
    ProjectionService projectionService(ProjectionRepository repository,
            TransactionTemplate routerTransactionTemplate, ConfigurationSnapshotClient snapshots) {
        return new ProjectionService(repository, routerTransactionTemplate, snapshots);
    }

    @Bean
    RoutingService routingService(RoutingRepository repository, TransactionTemplate routerTransactionTemplate) {
        return new RoutingService(repository, routerTransactionTemplate, 100, 20000);
    }

    @Bean
    HttpEventReferenceVerifier eventReferenceVerifier(Environment environment, ObjectMapper json) {
        return new HttpEventReferenceVerifier(
                URI.create(environment.getRequiredProperty("relayforge.ingestion.internal-url")),
                environment.getRequiredProperty("relayforge.router.event-credential"), json,
                environment.getProperty("relayforge.router.delivery-shard-id", "dlv-a"));
    }

    @Bean
    EventRoutingHandler eventRoutingHandler(HttpEventReferenceVerifier verifier,
            ProjectionService projections, RoutingService routing, Environment environment) {
        return new EventRoutingHandler(verifier, projections, routing,
                environment.getProperty("relayforge.router.delivery-shard-id", "dlv-a"));
    }

    @Bean
    RouterKafkaLifecycle routerKafkaLifecycle(Environment environment, ObjectMapper json,
            ProjectionService projections, EventRoutingHandler events) {
        return new RouterKafkaLifecycle(environment.getRequiredProperty("relayforge.kafka.bootstrap-servers"),
                environment.getProperty("relayforge.router.delivery-shard-id", "dlv-a"), json,
                projections::applyChange, events);
    }
}

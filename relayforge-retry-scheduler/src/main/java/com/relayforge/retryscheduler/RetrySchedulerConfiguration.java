package com.relayforge.retryscheduler;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.MeterRegistry;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "relayforge.database.username")
@EnableScheduling
public class RetrySchedulerConfiguration {
    @Bean(destroyMethod = "close")
    HikariDataSource retrySchedulerDataSource(Environment environment) {
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
    JdbcTemplate retrySchedulerJdbcTemplate(DataSource retrySchedulerDataSource) {
        JdbcTemplate jdbc = new JdbcTemplate(retrySchedulerDataSource);
        jdbc.setQueryTimeout(4);
        return jdbc;
    }

    @Bean
    RetrySchedulerService retrySchedulerService(JdbcTemplate retrySchedulerJdbcTemplate,
            DataSource retrySchedulerDataSource) {
        TransactionTemplate transactions = new TransactionTemplate(
                new JdbcTransactionManager(retrySchedulerDataSource));
        transactions.setTimeout(5);
        return new RetrySchedulerService(retrySchedulerJdbcTemplate, transactions);
    }

    @Bean
    RetrySchedulerLoop retrySchedulerLoop(RetrySchedulerService scheduler) {
        return new RetrySchedulerLoop(scheduler);
    }

    @Bean
    RetrySchedulerMetrics retrySchedulerMetrics(JdbcTemplate retrySchedulerJdbcTemplate,
            MeterRegistry registry) {
        return new RetrySchedulerMetrics(retrySchedulerJdbcTemplate, registry);
    }
}

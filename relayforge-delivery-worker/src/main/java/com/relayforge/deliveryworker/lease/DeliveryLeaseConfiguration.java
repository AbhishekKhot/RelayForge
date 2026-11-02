package com.relayforge.deliveryworker.lease;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
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
}

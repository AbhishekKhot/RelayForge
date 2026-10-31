package com.relayforge.deliveryapi.summary;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "relayforge.database.username")
public class DeliverySummaryConfiguration {
    @Bean(destroyMethod = "close")
    HikariDataSource deliveryDataSource(Environment environment) {
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
    JdbcTemplate deliveryJdbcTemplate(DataSource deliveryDataSource) {
        JdbcTemplate jdbc = new JdbcTemplate(deliveryDataSource);
        jdbc.setQueryTimeout(4);
        return jdbc;
    }

    @Bean
    DeliverySummaryRepository deliverySummaryRepository(JdbcTemplate deliveryJdbcTemplate) {
        return new JdbcDeliverySummaryRepository(deliveryJdbcTemplate);
    }
}

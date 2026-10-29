package com.relayforge.controlplane;

import com.relayforge.controlplane.organization.JdbcOrganizationRepository;
import com.relayforge.controlplane.organization.OrganizationRepository;
import com.relayforge.controlplane.organization.OrganizationService;
import com.relayforge.controlplane.security.ApiKeyCodec;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.security.SecureRandom;
import java.time.Clock;
import java.util.HexFormat;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "relayforge.database.username")
public class ControlPlaneDataConfiguration {
    @Bean(destroyMethod = "close")
    HikariDataSource controlDataSource(Environment environment) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(environment.getRequiredProperty("relayforge.database.url"));
        config.setUsername(environment.getRequiredProperty("relayforge.database.username"));
        config.setPassword(environment.getRequiredProperty("relayforge.database.password"));
        config.setMaximumPoolSize(8);
        config.setMinimumIdle(1);
        config.setConnectionTimeout(3000);
        return new HikariDataSource(config);
    }

    @Bean
    JdbcTemplate controlJdbcTemplate(DataSource controlDataSource) {
        return new JdbcTemplate(controlDataSource);
    }

    @Bean
    TransactionTemplate controlTransactionTemplate(DataSource controlDataSource) {
        return new TransactionTemplate(new JdbcTransactionManager(controlDataSource));
    }

    @Bean
    ApiKeyCodec apiKeyCodec(Environment environment) {
        String hex = environment.getRequiredProperty("relayforge.security.api-key-hmac-key");
        if (!hex.matches("[0-9a-fA-F]{64}")) {
            throw new IllegalStateException("API key HMAC key must be 32 random bytes in hexadecimal");
        }
        return new ApiKeyCodec(new SecureRandom(), HexFormat.of().parseHex(hex));
    }

    @Bean
    OrganizationRepository organizationRepository(JdbcTemplate controlJdbcTemplate) {
        return new JdbcOrganizationRepository(controlJdbcTemplate);
    }

    @Bean
    OrganizationService organizationService(OrganizationRepository repository, ApiKeyCodec codec,
            TransactionTemplate controlTransactionTemplate, ObjectMapper objectMapper) {
        return new OrganizationService(repository, codec, controlTransactionTemplate, objectMapper, Clock.systemUTC());
    }
}

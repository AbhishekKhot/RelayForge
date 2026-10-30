package com.relayforge.controlplane;

import com.relayforge.controlplane.organization.JdbcOrganizationRepository;
import com.relayforge.controlplane.organization.OrganizationRepository;
import com.relayforge.controlplane.organization.OrganizationService;
import com.relayforge.controlplane.endpoint.EndpointRegistrationService;
import com.relayforge.controlplane.endpoint.EndpointRepository;
import com.relayforge.controlplane.endpoint.JdbcEndpointRepository;
import com.relayforge.controlplane.endpoint.auth.EndpointAuthValidator;
import com.relayforge.controlplane.endpoint.auth.EndpointSecretCipher;
import com.relayforge.controlplane.endpoint.auth.EndpointKeyCheckService;
import com.relayforge.controlplane.endpoint.auth.KeyEncryptionService;
import com.relayforge.controlplane.security.ApiKeyCodec;
import com.relayforge.outbound.DestinationPolicy;
import com.relayforge.outbound.SafeWebhookClient;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.security.SecureRandom;
import java.net.InetAddress;
import java.net.URI;
import java.time.Clock;
import java.util.HexFormat;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.beans.factory.InitializingBean;

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

    @Bean
    DestinationPolicy destinationPolicy(Environment environment) {
        boolean local = environment.acceptsProfiles(Profiles.of("local"));
        String simulatorUrl = environment.getProperty("relayforge.ssrf.local-simulator-url");
        URI simulator = local && simulatorUrl != null ? URI.create(simulatorUrl) : null;
        return new DestinationPolicy(InetAddress::getAllByName, local, simulator);
    }

    @Bean(destroyMethod = "close")
    SafeWebhookClient safeWebhookClient(DestinationPolicy policy) {
        return new SafeWebhookClient(policy);
    }

    @Bean
    EndpointRepository endpointRepository(JdbcTemplate controlJdbcTemplate) {
        return new JdbcEndpointRepository(controlJdbcTemplate);
    }

    @Bean
    EndpointAuthValidator endpointAuthValidator(DestinationPolicy policy) {
        return new EndpointAuthValidator(policy);
    }

    @Bean
    @DependsOn("ownerMigration")
    InitializingBean endpointEncryptionKeyCheck(JdbcTemplate controlJdbcTemplate,
            KeyEncryptionService wrapping) {
        EndpointKeyCheckService checks = new EndpointKeyCheckService(controlJdbcTemplate, wrapping);
        return checks::verifyOnStartup;
    }

    @Bean
    EndpointRegistrationService endpointRegistrationService(EndpointRepository repository,
            DestinationPolicy policy, TransactionTemplate controlTransactionTemplate, ObjectMapper objectMapper,
            EndpointAuthValidator authValidator, EndpointSecretCipher secretCipher) {
        return new EndpointRegistrationService(repository, policy, controlTransactionTemplate,
                objectMapper, Clock.systemUTC(), authValidator, secretCipher);
    }
}

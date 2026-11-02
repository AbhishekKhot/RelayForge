package com.relayforge.deliveryapi.history;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.deliveryapi.replay.DeliveryReplayService;
import java.net.URI;
import java.time.Clock;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import javax.sql.DataSource;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "relayforge.database.username")
public class DeliveryHistoryConfiguration {
    @Bean
    DeliveryHistoryRepository deliveryHistoryRepository(JdbcTemplate deliveryJdbcTemplate) {
        return new JdbcDeliveryHistoryRepository(deliveryJdbcTemplate);
    }

    @Bean
    DeliveryQueryService deliveryQueryService(DeliveryHistoryRepository repository) {
        return new DeliveryQueryService(repository, Clock.systemUTC());
    }

    @Bean
    DeliveryApiAuthenticator deliveryApiAuthenticator(Environment environment, ObjectMapper json) {
        return new DeliveryApiAuthenticator(
                URI.create(environment.getRequiredProperty("relayforge.control.internal-url")), json);
    }

    @Bean
    DeliveryReplayService deliveryReplayService(JdbcTemplate deliveryJdbcTemplate,
            DataSource deliveryDataSource) {
        return new DeliveryReplayService(deliveryJdbcTemplate,
                new TransactionTemplate(new DataSourceTransactionManager(deliveryDataSource)),
                Clock.systemUTC());
    }
}

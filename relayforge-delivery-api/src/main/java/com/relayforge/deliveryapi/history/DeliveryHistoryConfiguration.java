package com.relayforge.deliveryapi.history;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.time.Clock;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;

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
}

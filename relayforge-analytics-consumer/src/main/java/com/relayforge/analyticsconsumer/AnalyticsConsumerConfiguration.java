package com.relayforge.analyticsconsumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.contracts.EnvelopeValidator;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration(proxyBeanMethods = false)
public class AnalyticsConsumerConfiguration {
    @Bean
    ClickHouseFactStore analyticsFactStore(Environment environment, ObjectMapper json) {
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1))
                .followRedirects(HttpClient.Redirect.NEVER).build();
        return new ClickHouseFactStore(
                URI.create(environment.getRequiredProperty("relayforge.clickhouse.url")),
                environment.getRequiredProperty("relayforge.clickhouse.username"),
                environment.getRequiredProperty("relayforge.clickhouse.password"), json, client);
    }

    @Bean
    AnalyticsKafkaLifecycle analyticsKafkaLifecycle(Environment environment,
            ObjectMapper json, ClickHouseFactStore store, MeterRegistry metrics) {
        return new AnalyticsKafkaLifecycle(
                environment.getRequiredProperty("relayforge.kafka.bootstrap-servers"),
                new EnvelopeValidator(json), store, metrics);
    }
}

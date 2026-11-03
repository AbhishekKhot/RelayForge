package com.relayforge.analyticsapi;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "relayforge.clickhouse.url")
public class AnalyticsApiConfiguration {
    @Bean
    AnalyticsAuthenticator analyticsAuthenticator(Environment environment, ObjectMapper json) {
        return new AnalyticsAuthenticator(URI.create(environment.getRequiredProperty(
                "relayforge.control.internal-url")), json);
    }

    @Bean
    AnalyticsRepository analyticsRepository(Environment environment, ObjectMapper json) {
        return new ClickHouseAnalyticsRepository(URI.create(environment.getRequiredProperty(
                "relayforge.clickhouse.url")), environment.getRequiredProperty(
                "relayforge.clickhouse.username"), environment.getRequiredProperty(
                "relayforge.clickhouse.password"), json, HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2)).followRedirects(HttpClient.Redirect.NEVER)
                .build());
    }

    @Bean
    AnalyticsService analyticsService(AnalyticsRepository repository) {
        return new AnalyticsService(repository);
    }
}

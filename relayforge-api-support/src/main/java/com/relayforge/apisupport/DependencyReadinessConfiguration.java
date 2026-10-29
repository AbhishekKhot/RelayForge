package com.relayforge.apisupport;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.Base64;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration(proxyBeanMethods = false)
public class DependencyReadinessConfiguration {
    @Bean
    @ConditionalOnProperty(name = "relayforge.readiness.postgres", havingValue = "true")
    ReadinessProbe postgresReadinessProbe(Environment environment) {
        String url = environment.getRequiredProperty("relayforge.database.url");
        String username = environment.getRequiredProperty("relayforge.database.username");
        String password = environment.getRequiredProperty("relayforge.database.password");
        return () -> {
            Properties properties = new Properties();
            properties.setProperty("user", username);
            properties.setProperty("password", password);
            properties.setProperty("connectTimeout", "2");
            properties.setProperty("socketTimeout", "2");
            try (var connection = DriverManager.getConnection(url, properties);
                    var statement = connection.createStatement()) {
                statement.setQueryTimeout(2);
                try (var result = statement.executeQuery("SELECT 1")) {
                    return result.next() && result.getInt(1) == 1;
                }
            }
        };
    }

    @Bean
    @ConditionalOnProperty(name = "relayforge.readiness.kafka", havingValue = "true")
    ReadinessProbe kafkaReadinessProbe(Environment environment) {
        String servers = environment.getRequiredProperty("relayforge.kafka.bootstrap-servers");
        return () -> {
            Properties properties = new Properties();
            properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            properties.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "2000");
            properties.setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "2000");
            try (AdminClient admin = AdminClient.create(properties)) {
                return !admin.describeCluster().nodes().get(2, TimeUnit.SECONDS).isEmpty();
            }
        };
    }

    @Bean
    @ConditionalOnProperty(name = "relayforge.readiness.clickhouse", havingValue = "true")
    ReadinessProbe clickHouseReadinessProbe(Environment environment) {
        String url = environment.getRequiredProperty("relayforge.clickhouse.url");
        String username = environment.getRequiredProperty("relayforge.clickhouse.username");
        String password = environment.getRequiredProperty("relayforge.clickhouse.password");
        String credentials = Base64.getEncoder().encodeToString(
                (username + ":" + password).getBytes(StandardCharsets.UTF_8));
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        URI uri = URI.create(url + "/?query=SELECT%201");
        return () -> {
            HttpRequest request = HttpRequest.newBuilder(uri)
                    .header("Authorization", "Basic " + credentials)
                    .timeout(Duration.ofSeconds(2))
                    .GET()
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return response.statusCode() == 200 && "1".equals(response.body().trim());
        };
    }
}

package com.relayforge.ingestion;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.DriverManager;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class IngestionMigrationIT {
    private static final PostgreSQLContainer<?> DATABASE = new PostgreSQLContainer<>("postgres:17.11")
            .withDatabaseName("relayforge_ingestion")
            .withUsername("relayforge")
            .withPassword("relayforge-test");

    static {
        DATABASE.start();
        awaitHostPort();
    }

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.flyway.url", DATABASE::getJdbcUrl);
        registry.add("spring.flyway.user", DATABASE::getUsername);
        registry.add("spring.flyway.password", DATABASE::getPassword);
    }

    @Autowired
    private Flyway flyway;

    @Test
    void startupMigratesEmptyDatabase() throws Exception {
        assertThat(flyway.info().current().getVersion().toString()).isEqualTo("2");
        try (var connection = DriverManager.getConnection(
                DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword());
                var statement = connection.createStatement();
                var result = statement.executeQuery("SELECT count(*) FROM events")) {
            result.next();
            assertThat(result.getLong(1)).isZero();
        }
    }

    private static void awaitHostPort() {
        for (int attempt = 0; attempt < 40; attempt++) {
            try (var connection = DriverManager.getConnection(
                    DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword())) {
                return;
            } catch (SQLException unavailable) {
                try {
                    Thread.sleep(250);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
            }
        }
        throw new IllegalStateException("PostgreSQL host port did not become available");
    }
}

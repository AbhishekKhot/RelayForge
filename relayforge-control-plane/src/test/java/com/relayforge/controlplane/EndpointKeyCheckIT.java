package com.relayforge.controlplane;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EndpointKeyCheckIT {
    private static final PostgreSQLContainer<?> DATABASE = new PostgreSQLContainer<>("postgres:17.11")
            .withDatabaseName("relayforge_control")
            .withUsername("relayforge")
            .withPassword("relayforge-test");
    private static final String FIRST = "0123456789abcdef".repeat(4);
    private static final String WRONG = "fedcba9876543210".repeat(4);

    static {
        DATABASE.start();
    }

    @Test
    void refusesStartupWhenExistingCanaryHasSameVersionButDifferentKey() {
        try (ConfigurableApplicationContext started = start(FIRST)) {
            assertThat(started.isActive()).isTrue();
        }
        assertThatThrownBy(() -> start(WRONG))
                .isInstanceOf(Exception.class)
                .hasMessageNotContaining(FIRST)
                .hasMessageNotContaining(WRONG);
        try (ConfigurableApplicationContext restored = start(FIRST)) {
            assertThat(restored.isActive()).isTrue();
        }
    }

    private ConfigurableApplicationContext start(String key) {
        return new SpringApplicationBuilder(ControlPlaneApplication.class)
                .web(WebApplicationType.NONE)
                .profiles("local")
                .properties(Map.ofEntries(
                        Map.entry("spring.flyway.url", DATABASE.getJdbcUrl()),
                        Map.entry("spring.flyway.user", DATABASE.getUsername()),
                        Map.entry("spring.flyway.password", DATABASE.getPassword()),
                        Map.entry("relayforge.database.url", DATABASE.getJdbcUrl()),
                        Map.entry("relayforge.database.username", DATABASE.getUsername()),
                        Map.entry("relayforge.database.password", DATABASE.getPassword()),
                        Map.entry("relayforge.provisioning.credential", "trusted-provisioning-credential-for-tests-12345"),
                        Map.entry("relayforge.security.api-key-hmac-key", FIRST),
                        Map.entry("relayforge.encryption.local-keys", "local-v1:" + key),
                        Map.entry("relayforge.encryption.active-version", "local-v1")))
                .run();
    }
}

package com.relayforge.controlplane.endpoint.auth;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

class EndpointEncryptionConfigurationTest {
    private final ApplicationContextRunner contexts = new ApplicationContextRunner()
            .withUserConfiguration(EndpointEncryptionConfiguration.class)
            .withPropertyValues("relayforge.database.username=test", "spring.profiles.active=local");

    @Test
    void failsStartupWithoutOrWithWrongLocalKey() {
        contexts.run(context -> assertThat(context).hasFailed());
        contexts.withPropertyValues("relayforge.encryption.local-keys=local-v1:bad",
                "relayforge.encryption.active-version=local-v1")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void startsOnlyWithValidLocalKeyAndSelectedVersion() {
        contexts.withPropertyValues("relayforge.encryption.local-keys=local-v1:"
                + "0123456789abcdef".repeat(4), "relayforge.encryption.active-version=local-v1")
                .run(context -> assertThat(context).hasSingleBean(EndpointSecretCipher.class));
    }

    @Test
    void refusesLocalProviderOutsideLocalAndTestProfiles() {
        new ApplicationContextRunner().withUserConfiguration(EndpointEncryptionConfiguration.class)
                .withPropertyValues("relayforge.database.username=test",
                        "relayforge.encryption.local-keys=local-v1:" + "0123456789abcdef".repeat(4),
                        "relayforge.encryption.active-version=local-v1")
                .run(context -> assertThat(context).hasFailed());
    }
}

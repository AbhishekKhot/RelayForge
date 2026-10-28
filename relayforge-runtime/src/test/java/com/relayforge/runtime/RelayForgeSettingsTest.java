package com.relayforge.runtime;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

class RelayForgeSettingsTest {
    private final ApplicationContextRunner context = new ApplicationContextRunner()
            .withUserConfiguration(RuntimeConfiguration.class);

    @Test
    void bindsValidOverrides() {
        context.withPropertyValues("relayforge.api.max-event-bytes=131072", "relayforge.retry.max-attempts=5")
                .run(result -> {
                    assertThat(result).hasNotFailed();
                    RelayForgeSettings settings = result.getBean(RelayForgeSettings.class);
                    assertThat(settings.getApi().getMaxEventBytes()).isEqualTo(131072);
                    assertThat(settings.getRetry().getMaxAttempts()).isEqualTo(5);
                });
    }

    @Test
    void rejectsUnsafeLimits() {
        context.withPropertyValues("relayforge.api.max-event-bytes=0")
                .run(result -> assertThat(result).hasFailed());
    }

    @Test
    void rejectsInvalidTimeoutOrder() {
        context.withPropertyValues("relayforge.webhook.overall-timeout=1s")
                .run(result -> assertThat(result).hasFailed());
    }

    @Test
    void rejectsShortProvisioningCredential() {
        context.withPropertyValues("relayforge.provisioning.credential=short")
                .run(result -> assertThat(result).hasFailed());
    }

    @Test
    void rejectsLocalKeyProviderInProduction() {
        context.withPropertyValues("spring.profiles.active=prod")
                .run(result -> assertThat(result).hasFailed());
    }

    @Test
    void requiresControlPlaneProvisioningCredentialInProduction() {
        context.withPropertyValues("spring.profiles.active=prod",
                        "spring.application.name=relayforge-control-plane",
                        "relayforge.encryption.provider=managed")
                .run(result -> assertThat(result).hasFailed());
    }

    @Test
    void acceptsManagedProviderAndProvisioningCredentialInProduction() {
        context.withPropertyValues("spring.profiles.active=prod",
                        "spring.application.name=relayforge-control-plane",
                        "relayforge.encryption.provider=managed",
                        "relayforge.provisioning.credential=0123456789abcdef0123456789abcdef")
                .run(result -> assertThat(result).hasNotFailed());
    }
}

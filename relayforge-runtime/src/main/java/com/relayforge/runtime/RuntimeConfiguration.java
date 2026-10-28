package com.relayforge.runtime;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(RelayForgeSettings.class)
public class RuntimeConfiguration {
    @Bean
    SmartInitializingSingleton productionKeyProviderGuard(Environment environment, RelayForgeSettings settings) {
        return () -> {
            if (environment.acceptsProfiles(Profiles.of("prod"))) {
                if ("local".equals(settings.getEncryption().getProvider())) {
                    throw new IllegalStateException("A managed encryption key provider is required in production");
                }
                if ("relayforge-control-plane".equals(environment.getProperty("spring.application.name"))
                        && settings.getProvisioning().getCredential() == null) {
                    throw new IllegalStateException("A provisioning credential is required in production");
                }
            }
        };
    }
}

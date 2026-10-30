package com.relayforge.controlplane.endpoint.auth;

import java.security.SecureRandom;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "relayforge.database.username")
public class EndpointEncryptionConfiguration {
    @Bean
    KeyEncryptionService keyEncryptionService(Environment environment) {
        boolean local = environment.acceptsProfiles(Profiles.of("local", "test"));
        return LocalKeyEncryptionService.fromSettings(
                environment.getProperty("relayforge.encryption.local-keys"),
                environment.getProperty("relayforge.encryption.active-version"), local);
    }

    @Bean
    EndpointSecretCipher endpointSecretCipher(KeyEncryptionService wrapping) {
        return new EndpointSecretCipher(wrapping, new SecureRandom());
    }
}

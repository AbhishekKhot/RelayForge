package com.relayforge.dbmigration;

import org.flywaydb.core.Flyway;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
@ConditionalOnProperty(name = "spring.flyway.url")
public class OwnerMigrationConfiguration {
    @Bean
    Flyway ownerFlyway(Environment environment) {
        return Flyway.configure()
                .dataSource(
                        environment.getRequiredProperty("spring.flyway.url"),
                        environment.getRequiredProperty("spring.flyway.user"),
                        environment.getRequiredProperty("spring.flyway.password"))
                .locations(environment.getProperty("spring.flyway.locations", "classpath:db/migration"))
                .load();
    }

    @Bean
    InitializingBean ownerMigration(Flyway ownerFlyway) {
        return () -> ownerFlyway.migrate();
    }
}

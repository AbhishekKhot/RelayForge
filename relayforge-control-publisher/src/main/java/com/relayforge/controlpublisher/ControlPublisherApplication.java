package com.relayforge.controlpublisher;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(scanBasePackages = "com.relayforge", exclude = DataSourceAutoConfiguration.class)
public class ControlPublisherApplication {
    public static void main(String[] args) {
        SpringApplication.run(ControlPublisherApplication.class, args);
    }
}

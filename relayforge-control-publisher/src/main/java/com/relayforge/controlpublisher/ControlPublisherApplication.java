package com.relayforge.controlpublisher;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.relayforge")
public class ControlPublisherApplication {
    public static void main(String[] args) {
        SpringApplication.run(ControlPublisherApplication.class, args);
    }
}

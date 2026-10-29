package com.relayforge.ingestionpublisher;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.relayforge")
public class IngestionPublisherApplication {
    public static void main(String[] args) {
        SpringApplication.run(IngestionPublisherApplication.class, args);
    }
}

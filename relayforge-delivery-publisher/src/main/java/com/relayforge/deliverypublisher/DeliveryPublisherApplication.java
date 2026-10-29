package com.relayforge.deliverypublisher;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.relayforge")
public class DeliveryPublisherApplication {
    public static void main(String[] args) {
        SpringApplication.run(DeliveryPublisherApplication.class, args);
    }
}

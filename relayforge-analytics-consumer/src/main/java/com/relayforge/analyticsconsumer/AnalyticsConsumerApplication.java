package com.relayforge.analyticsconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.relayforge")
public class AnalyticsConsumerApplication {
    public static void main(String[] args) {
        SpringApplication.run(AnalyticsConsumerApplication.class, args);
    }
}

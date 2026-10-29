package com.relayforge.retryscheduler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.relayforge")
public class RetrySchedulerApplication {
    public static void main(String[] args) {
        SpringApplication.run(RetrySchedulerApplication.class, args);
    }
}

package com.relayforge.deliveryworker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(scanBasePackages = "com.relayforge", exclude = DataSourceAutoConfiguration.class)
public class DeliveryWorkerApplication {
    public static void main(String[] args) {
        SpringApplication.run(DeliveryWorkerApplication.class, args);
    }
}

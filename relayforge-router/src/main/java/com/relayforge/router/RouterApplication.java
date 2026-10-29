package com.relayforge.router;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.relayforge")
public class RouterApplication {
    public static void main(String[] args) {
        SpringApplication.run(RouterApplication.class, args);
    }
}

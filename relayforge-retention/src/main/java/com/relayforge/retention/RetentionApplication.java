package com.relayforge.retention;

import java.time.Clock;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

public final class RetentionApplication {
    private RetentionApplication() {
    }

    public static void main(String[] args) {
        if (args.length != 0) {
            throw new IllegalArgumentException("No arguments are supported");
        }
        var ingestion = new DriverManagerDataSource(
                required("INGESTION_JDBC_URL"), required("INGESTION_DB_USER"),
                required("INGESTION_DB_PASSWORD"));
        var delivery = new DriverManagerDataSource(
                required("DELIVERY_JDBC_URL"), required("DELIVERY_DB_USER"),
                required("DELIVERY_DB_PASSWORD"));
        RetentionService.Report report = new RetentionService(ingestion, delivery,
                Clock.systemUTC()).runOnce();
        System.out.println("Retention days released=" + report.releasedDays()
                + " blocked=" + report.blockedDays()
                + " oldest_blocked_day=" + report.oldestBlockedDay());
        if (report.blockedDays() > 0) {
            System.exit(1);
        }
    }

    private static String required(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing retention configuration: " + name);
        }
        return value;
    }
}

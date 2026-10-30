package com.relayforge.ingestion.event;

import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.server.ResponseStatusException;

public final class PostgresDurabilityGate implements DurabilityGate {
    private final JdbcTemplate jdbc;
    private final boolean acceptanceEnabled;
    private final String standbyName;

    public PostgresDurabilityGate(JdbcTemplate jdbc, boolean acceptanceEnabled, String standbyName) {
        this.jdbc = jdbc;
        this.acceptanceEnabled = acceptanceEnabled;
        this.standbyName = standbyName;
    }

    @Override
    public void verify() {
        if (!acceptanceEnabled || standbyName == null || standbyName.isBlank()) {
            throw unavailable();
        }
        Boolean ready = jdbc.queryForObject("""
                SELECT current_setting('fsync') = 'on'
                    AND current_setting('synchronous_commit') = 'on'
                    AND current_setting('synchronous_standby_names') <> ''
                    AND EXISTS (SELECT 1 FROM pg_stat_replication
                                WHERE application_name = ? AND sync_state IN ('sync', 'quorum'))
                """, Boolean.class, standbyName);
        if (!Boolean.TRUE.equals(ready)) {
            throw unavailable();
        }
    }

    private ResponseStatusException unavailable() {
        return new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
    }
}

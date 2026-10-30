package com.relayforge.controlplane.endpoint;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;

public final class JdbcEndpointRepository implements EndpointRepository {
    private final JdbcTemplate jdbc;

    public JdbcEndpointRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void insert(UUID id, UUID organizationId, String name, String baseUrl, String path,
            String queryJson, String headersJson, String metadataJson,
            int timeoutMs, int maxConcurrency, int maxAttempts, Instant createdAt) {
        jdbc.update("""
                INSERT INTO endpoints (id, organization_id, name, base_url, path, query_params, headers,
                  metadata, auth_type, timeout_ms, max_concurrency, max_attempts, created_at)
                VALUES (?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?::jsonb, 'NONE', ?, ?, ?, ?)
                """, id, organizationId, name, baseUrl, path, queryJson, headersJson, metadataJson,
                timeoutMs, maxConcurrency, maxAttempts, Timestamp.from(createdAt));
    }

    @Override
    public void insertSubscription(UUID id, UUID organizationId, UUID endpointId, String eventType) {
        jdbc.update("""
                INSERT INTO subscriptions (id, organization_id, endpoint_id, event_type)
                VALUES (?, ?, ?, ?)
                """, id, organizationId, endpointId, eventType);
    }

    @Override
    public void insertAudit(UUID organizationId, UUID endpointId, String requestId) {
        jdbc.update("""
                INSERT INTO audit_records (organization_id, action, resource_type, resource_id, request_id)
                VALUES (?, 'endpoint.created', 'endpoint', ?, ?)
                """, organizationId, endpointId, requestId);
    }
}

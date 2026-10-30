package com.relayforge.controlplane;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.DriverManager;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class OrganizationProvisioningIT {
    private static final String PROVISIONING_CREDENTIAL = "trusted-provisioning-credential-for-tests-12345";
    private static final PostgreSQLContainer<?> DATABASE = new PostgreSQLContainer<>("postgres:17.11")
            .withDatabaseName("relayforge_control")
            .withUsername("relayforge")
            .withPassword("relayforge-test");

    static {
        DATABASE.start();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.flyway.url", DATABASE::getJdbcUrl);
        registry.add("spring.flyway.user", DATABASE::getUsername);
        registry.add("spring.flyway.password", DATABASE::getPassword);
        registry.add("relayforge.database.url", DATABASE::getJdbcUrl);
        registry.add("relayforge.database.username", DATABASE::getUsername);
        registry.add("relayforge.database.password", DATABASE::getPassword);
        registry.add("relayforge.provisioning.credential", () -> PROVISIONING_CREDENTIAL);
        registry.add("relayforge.security.api-key-hmac-key", () -> "0123456789abcdef".repeat(4));
    }

    @Autowired
    private TestRestTemplate client;

    @Test
    void createsOrganizationAndReturnsUsableKeyOnceWithoutPersistingPlaintext() throws Exception {
        ResponseEntity<JsonNode> denied = post("/v1/organizations", null,
                Map.of("name", "Denied"));
        assertThat(denied.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);

        ResponseEntity<JsonNode> created = post("/v1/organizations", PROVISIONING_CREDENTIAL,
                Map.of("name", "Acme Payments", "plan_code", "FREE"));
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String organizationId = created.getBody().path("data").path("organization_id").asText();
        String key = created.getBody().path("data").path("api_key").asText();
        assertThat(key).startsWith("rf_live_");
        assertThat(created.getBody().path("data").path("api_key_warning").asText()).contains("once");

        try (var connection = DriverManager.getConnection(
                DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword());
                var statement = connection.prepareStatement("SELECT secret_hash, scopes FROM api_keys WHERE organization_id = ?")) {
            statement.setObject(1, java.util.UUID.fromString(organizationId));
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getBytes(1)).hasSize(32);
                assertThat(new String(rows.getBytes(1))).doesNotContain(key);
                assertThat(rows.next()).isFalse();
            }
        }

        ResponseEntity<JsonNode> accepted = get("/v1/auth/check", key);
        assertThat(accepted.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(accepted.getBody().path("organization_id").asText()).isEqualTo(organizationId);

        ResponseEntity<JsonNode> wrong = get("/v1/auth/check", key + "wrong");
        assertThat(wrong.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void provisionsUsersOnlyForExistingOrganizationsAndRejectsDuplicates() throws Exception {
        ResponseEntity<JsonNode> organization = post("/v1/organizations", PROVISIONING_CREDENTIAL,
                Map.of("name", "User Owner"));
        String organizationId = organization.getBody().path("data").path("organization_id").asText();
        Map<String, Object> user = Map.of("organization_id", organizationId,
                "email", "Operator@Example.com", "name", "Operations", "metadata", Map.of("team", "ops"));

        assertThat(post("/v1/users", null, user).getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        ResponseEntity<JsonNode> created = post("/v1/users", PROVISIONING_CREDENTIAL, user);
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(post("/v1/users", PROVISIONING_CREDENTIAL, user).getStatusCode())
                .isEqualTo(HttpStatus.CONFLICT);
        assertThat(post("/v1/users", PROVISIONING_CREDENTIAL,
                Map.of("organization_id", UUID.randomUUID().toString(), "email", "nobody@example.com",
                        "name", "Nobody")).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);

        try (var connection = DriverManager.getConnection(
                DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword());
                var statement = connection.prepareStatement(
                        "SELECT email, metadata->>'team' FROM users WHERE organization_id = ?")) {
            statement.setObject(1, UUID.fromString(organizationId));
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString(1)).isEqualTo("operator@example.com");
                assertThat(rows.getString(2)).isEqualTo("ops");
                assertThat(rows.next()).isFalse();
            }
        }
    }

    @Test
    void rotatesAndRevokesKeysWithScopeChecksAndAuditRecords() throws Exception {
        ResponseEntity<JsonNode> organization = post("/v1/organizations", PROVISIONING_CREDENTIAL,
                Map.of("name", "Key Owner"));
        String organizationId = organization.getBody().path("data").path("organization_id").asText();
        String initialKey = organization.getBody().path("data").path("api_key").asText();
        String initialId = initialKey.split("_")[2];

        ResponseEntity<JsonNode> rotated = post("/v1/api-keys/" + initialId + "/rotate",
                PROVISIONING_CREDENTIAL, Map.of("scopes", java.util.List.of("events:read")));
        assertThat(rotated.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String readOnlyKey = rotated.getBody().path("data").path("api_key").asText();
        assertThat(get("/v1/auth/check", initialKey).getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(get("/v1/auth/check", readOnlyKey).getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);

        String readOnlyId = readOnlyKey.split("_")[2];
        ResponseEntity<JsonNode> second = post("/v1/api-keys/" + readOnlyId + "/rotate",
                PROVISIONING_CREDENTIAL, Map.of("scopes", java.util.List.of("events:write")));
        String activeKey = second.getBody().path("data").path("api_key").asText();
        assertThat(get("/v1/auth/check", activeKey).getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(post("/v1/api-keys/" + activeKey.split("_")[2] + "/revoke",
                PROVISIONING_CREDENTIAL, Map.of()).getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
        assertThat(get("/v1/auth/check", activeKey).getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);

        try (var connection = DriverManager.getConnection(
                DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword());
                var statement = connection.prepareStatement(
                        "SELECT count(*) FROM audit_records WHERE organization_id = ? AND action LIKE 'api_key.%'")) {
            statement.setObject(1, UUID.fromString(organizationId));
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getInt(1)).isEqualTo(3);
            }
        }
    }

    @Test
    void rejectsExpiredKeysAndSuspendedOrganizations() throws Exception {
        ResponseEntity<JsonNode> organization = post("/v1/organizations", PROVISIONING_CREDENTIAL,
                Map.of("name", "Expiring Owner"));
        String organizationId = organization.getBody().path("data").path("organization_id").asText();
        String key = organization.getBody().path("data").path("api_key").asText();

        try (var connection = DriverManager.getConnection(
                DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword());
                var statement = connection.prepareStatement(
                        "UPDATE api_keys SET expires_at = clock_timestamp() - interval '1 second' WHERE key_id = ?")) {
            statement.setString(1, key.split("_")[2]);
            assertThat(statement.executeUpdate()).isEqualTo(1);
        }
        assertThat(get("/v1/auth/check", key).getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);

        try (var connection = DriverManager.getConnection(
                DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword());
                var statement = connection.prepareStatement("UPDATE api_keys SET expires_at = NULL WHERE key_id = ?")) {
            statement.setString(1, key.split("_")[2]);
            assertThat(statement.executeUpdate()).isEqualTo(1);
        }
        assertThat(get("/v1/auth/check", key).getStatusCode()).isEqualTo(HttpStatus.OK);

        try (var connection = DriverManager.getConnection(
                DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword());
                var statement = connection.prepareStatement("UPDATE organizations SET status = 'SUSPENDED' WHERE id = ?")) {
            statement.setObject(1, UUID.fromString(organizationId));
            assertThat(statement.executeUpdate()).isEqualTo(1);
        }
        assertThat(get("/v1/auth/check", key).getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void cannotReadAnotherOrganizationsMetadata() {
        ResponseEntity<JsonNode> first = post("/v1/organizations", PROVISIONING_CREDENTIAL,
                Map.of("name", "First Tenant"));
        ResponseEntity<JsonNode> second = post("/v1/organizations", PROVISIONING_CREDENTIAL,
                Map.of("name", "Second Tenant"));
        String firstId = first.getBody().path("data").path("organization_id").asText();
        String secondId = second.getBody().path("data").path("organization_id").asText();
        String firstKey = first.getBody().path("data").path("api_key").asText();

        assertThat(get("/v1/organizations/" + firstId, firstKey).getStatusCode()).isEqualTo(HttpStatus.OK);
        ResponseEntity<JsonNode> other = get("/v1/organizations/" + secondId, firstKey);
        assertThat(other.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(other.getBody().toString()).doesNotContain("Second Tenant");
    }

    @Test
    void validatesQuotasBeforePersistingOrganization() throws Exception {
        ResponseEntity<JsonNode> rejected = post("/v1/organizations", PROVISIONING_CREDENTIAL,
                Map.of("name", "Invalid Quota", "quota_overrides", Map.of("events_per_minute", 60001)));
        assertThat(rejected.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        ResponseEntity<JsonNode> created = post("/v1/organizations", PROVISIONING_CREDENTIAL,
                Map.of("name", "Custom Quota", "quota_overrides", Map.of("events_per_minute", 1200)));
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String id = created.getBody().path("data").path("organization_id").asText();
        String key = created.getBody().path("data").path("api_key").asText();
        ResponseEntity<JsonNode> authorization = get("/v1/auth/check", key);
        assertThat(authorization.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(authorization.getBody().path("events_per_minute").asInt()).isEqualTo(1200);
        try (var connection = DriverManager.getConnection(
                DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword());
                var statement = connection.prepareStatement(
                        "SELECT (quota_config->>'events_per_minute')::integer, "
                                + "(quota_config->>'max_active_endpoints')::integer "
                                + "FROM organizations WHERE id = ?")) {
            statement.setObject(1, UUID.fromString(id));
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getInt(1)).isEqualTo(1200);
                assertThat(rows.getInt(2)).isEqualTo(25);
            }
        }
    }

    @Test
    void duplicateProvisioningRequestIsRecoverableWithoutReturningSecretAgain() {
        String idempotencyKey = UUID.randomUUID().toString();
        ResponseEntity<JsonNode> created = postOrganization(idempotencyKey, Map.of("name", "Recoverable"));
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String firstKey = created.getBody().path("data").path("api_key").asText();
        String organizationId = created.getBody().path("data").path("organization_id").asText();

        ResponseEntity<JsonNode> repeated = postOrganization(idempotencyKey, Map.of("name", "Recoverable"));
        assertThat(repeated.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(repeated.getBody().toString()).doesNotContain(firstKey);

        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Provisioning-Credential", PROVISIONING_CREDENTIAL);
        headers.set("Idempotency-Key", idempotencyKey);
        ResponseEntity<JsonNode> recovered = client.exchange("/v1/organizations/recover", HttpMethod.GET,
                new HttpEntity<>(headers), JsonNode.class);
        assertThat(recovered.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(recovered.getBody().path("data").path("organization_id").asText())
                .isEqualTo(organizationId);
        assertThat(recovered.getBody().path("data").path("key_id").asText())
                .isEqualTo(firstKey.split("_")[2]);
        assertThat(recovered.getBody().toString()).doesNotContain(firstKey);
    }

    private ResponseEntity<JsonNode> postOrganization(String idempotencyKey, Map<String, ?> body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-Provisioning-Credential", PROVISIONING_CREDENTIAL);
        headers.set("Idempotency-Key", idempotencyKey);
        return client.exchange("/v1/organizations", HttpMethod.POST, new HttpEntity<>(body, headers), JsonNode.class);
    }

    private ResponseEntity<JsonNode> post(String path, String credential, Map<String, ?> body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (credential != null) {
            headers.set("X-Provisioning-Credential", credential);
        }
        if (path.equals("/v1/organizations")) {
            headers.set("Idempotency-Key", UUID.randomUUID().toString());
        }
        return client.exchange(path, HttpMethod.POST, new HttpEntity<>(body, headers), JsonNode.class);
    }

    private ResponseEntity<JsonNode> get(String path, String key) {
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(key);
        return client.exchange(path, HttpMethod.GET, new HttpEntity<>(headers), JsonNode.class);
    }
}

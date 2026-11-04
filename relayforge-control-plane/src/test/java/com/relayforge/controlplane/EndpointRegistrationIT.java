package com.relayforge.controlplane;

import com.fasterxml.jackson.databind.JsonNode;
import com.relayforge.controlplane.endpoint.auth.EndpointSecretCipher;
import com.relayforge.controlplane.endpoint.auth.EndpointSecretCipher.EncryptedSecret;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("local")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class EndpointRegistrationIT {
    private static final String PROVISIONING_CREDENTIAL = "trusted-provisioning-credential-for-tests-12345";
    private static final PostgreSQLContainer<?> DATABASE = new PostgreSQLContainer<>("postgres:17.11")
            .withDatabaseName("relayforge_control")
            .withUsername("relayforge")
            .withPassword("relayforge-test");
    private static final HttpServer SIMULATOR;

    static {
        try {
            DATABASE.start();
            SIMULATOR = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            SIMULATOR.createContext("/hook", exchange -> {
                exchange.sendResponseHeaders(204, -1);
                exchange.close();
            });
            SIMULATOR.start();
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }

    @AfterAll
    static void stopSimulator() {
        SIMULATOR.stop(0);
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
        registry.add("relayforge.router.config-credential", () -> "test-config-credential-01234567890123456789");
        registry.add("relayforge.delivery.config-credential", () -> "test-delivery-config-credential-0123456789");
        registry.add("relayforge.security.api-key-hmac-key", () -> "0123456789abcdef".repeat(4));
        registry.add("relayforge.encryption.local-keys", () -> "local-v1:" + "fedcba9876543210".repeat(4));
        registry.add("relayforge.encryption.active-version", () -> "local-v1");
        registry.add("relayforge.ssrf.local-simulator-url",
                () -> "http://localhost:" + SIMULATOR.getAddress().getPort());
    }

    @Autowired
    private TestRestTemplate client;

    @Autowired
    private EndpointSecretCipher secretCipher;

    @Test
    void internalConfigurationSnapshotIsPagedTenantScopedAndSecretFree() throws Exception {
        Credentials owner = credentials();
        Credentials other = credentials();
        Map<String, Object> body = new HashMap<>(request(simulatorUrl()));
        body.put("authentication", Map.of("type", "BEARER", "token", "snapshot-private-token"));
        register(owner.key(), body);
        register(owner.key(), request(simulatorUrl()));
        register(other.key(), request(simulatorUrl()));
        String path = "/internal/v1/organizations/" + owner.organizationId() + "/configuration";
        assertThat(client.getForEntity(path + "/endpoints", JsonNode.class).getStatusCode())
                .isEqualTo(HttpStatus.UNAUTHORIZED);
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-RelayForge-Router-Config-Credential", "test-config-credential-01234567890123456789");
        ResponseEntity<JsonNode> endpoints = client.exchange(path + "/endpoints?limit=1",
                HttpMethod.GET, new HttpEntity<>(headers), JsonNode.class);
        assertThat(endpoints.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(endpoints.getBody().path("source_revision").asLong()).isEqualTo(2);
        assertThat(endpoints.getBody().path("data").size()).isEqualTo(1);
        assertThat(endpoints.getBody().path("next_cursor").asText()).isNotBlank();
        assertThat(endpoints.getBody().toString()).doesNotContain("snapshot-private-token", simulatorUrl(),
                other.organizationId().toString(), "ciphertext");
        ResponseEntity<JsonNode> subscriptions = client.exchange(path + "/subscriptions?limit=100",
                HttpMethod.GET, new HttpEntity<>(headers), JsonNode.class);
        assertThat(subscriptions.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(subscriptions.getBody().path("data").size()).isEqualTo(4);
        assertThat(subscriptions.getBody().toString()).doesNotContain(other.organizationId().toString());
    }

    @Test
    void internalDeliveryConfigurationRequiresCredentialAndTenantAndUsesCurrentSecret() throws Exception {
        Credentials owner = credentials();
        Credentials other = credentials();
        Map<String, Object> body = new HashMap<>(request(simulatorUrl()));
        body.put("authentication", Map.of("type", "BEARER", "token", "delivery-private-token"));
        ResponseEntity<JsonNode> created = register(owner.key(), body);
        String id = created.getBody().path("data").path("endpoint_id").asText();
        String initialSecret = created.getBody().path("data").path("signing_secret").asText();
        String path = "/internal/v1/organizations/" + owner.organizationId() + "/endpoints/" + id
                + "/delivery";
        assertThat(client.getForEntity(path, JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-RelayForge-Delivery-Config-Credential",
                "test-delivery-config-credential-0123456789");
        ResponseEntity<JsonNode> read = client.exchange(path, HttpMethod.GET,
                new HttpEntity<>(headers), JsonNode.class);
        assertThat(read.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(read.getBody().path("organization_id").asText()).isEqualTo(owner.organizationId().toString());
        assertThat(read.getBody().path("endpoint_id").asText()).isEqualTo(id);
        assertThat(read.getBody().path("signing_secret").asText()).isEqualTo(initialSecret);
        assertThat(read.getBody().path("authentication").path("token").asText())
                .isEqualTo("delivery-private-token");
        assertThat(client.exchange(path.replace(owner.organizationId().toString(),
                other.organizationId().toString()), HttpMethod.GET, new HttpEntity<>(headers),
                JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(readEndpoint(owner.key(), id).getBody().toString())
                .doesNotContain(initialSecret, "delivery-private-token");
    }

    @Test
    void registersValidatedSimulatorEndpointAndSubscriptionsAtomically() throws Exception {
        Credentials credentials = credentials();
        ResponseEntity<JsonNode> created = register(credentials.key(), request(simulatorUrl()));

        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String endpointId = created.getBody().path("data").path("endpoint_id").asText();
        String signingSecret = created.getBody().path("data").path("signing_secret").asText();
        assertThat(signingSecret).hasSizeGreaterThan(32);
        assertThat(created.getBody().toString()).doesNotContain(credentials.key());
        assertThat(readEndpoint(credentials.key(), endpointId).getBody().toString())
                .doesNotContain(signingSecret, "signing_secret");
        assertThat(count("SELECT count(*) FROM endpoints WHERE id = ? AND organization_id = ?",
                UUID.fromString(endpointId), credentials.organizationId())).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM subscriptions WHERE endpoint_id = ? AND organization_id = ?",
                UUID.fromString(endpointId), credentials.organizationId())).isEqualTo(2);
        try (var connection = DriverManager.getConnection(
                DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword());
                var statement = connection.prepareStatement("""
                        SELECT signing_secret_ciphertext, signing_wrapped_dek, signing_key_version,
                            signing_nonce, signing_secret_version FROM endpoints
                        WHERE id = ? AND organization_id = ?
                        """)) {
            statement.setObject(1, UUID.fromString(endpointId));
            statement.setObject(2, credentials.organizationId());
            try (var row = statement.executeQuery()) {
                assertThat(row.next()).isTrue();
                EncryptedSecret encrypted = new EncryptedSecret(row.getBytes(1), row.getBytes(2),
                        row.getString(3), row.getBytes(4));
                assertThat(row.getLong(5)).isEqualTo(1);
                assertThat(new String(secretCipher.decrypt(credentials.organizationId(),
                        UUID.fromString(endpointId), "signing_secret", 1, encrypted),
                        StandardCharsets.US_ASCII)).isEqualTo(signingSecret);
            }
        }
    }

    @Test
    void limitsActiveEndpointsUsingOrganizationQuota() {
        Credentials owner = credentials();
        HttpHeaders admin = new HttpHeaders();
        admin.setContentType(MediaType.APPLICATION_JSON);
        admin.set("X-Provisioning-Credential", PROVISIONING_CREDENTIAL);
        ResponseEntity<JsonNode> updated = client.exchange("/v1/organizations/"
                + owner.organizationId() + "/quotas", HttpMethod.PUT,
                new HttpEntity<>(Map.of("quota_overrides", Map.of("max_active_endpoints", 1)), admin),
                JsonNode.class);
        assertThat(updated.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(register(owner.key(), request(simulatorUrl())).getStatusCode())
                .isEqualTo(HttpStatus.CREATED);
        assertThat(register(owner.key(), request(simulatorUrl())).getStatusCode())
                .isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
    }

    @Test
    void rejectsMetadataAddressAndDoesNotPersistEndpoint() throws Exception {
        Credentials credentials = credentials();
        ResponseEntity<JsonNode> rejected = register(credentials.key(), request("https://169.254.169.254"));

        assertThat(rejected.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(rejected.getBody().toString()).doesNotContain("169.254.169.254");
        assertThat(count("SELECT count(*) FROM endpoints WHERE organization_id = ?",
                credentials.organizationId())).isZero();
    }

    @Test
    void rejectsMalformedHeadersPolicyAndDuplicateEventTypesBeforeWriting() throws Exception {
        Credentials credentials = credentials();
        Map<String, Object> badHeader = request(simulatorUrl(), Map.of("Host", "internal"),
                List.of("payment.completed", "payment.failed"), 10000);
        Map<String, Object> badTimeout = request(simulatorUrl(), Map.of(),
                List.of("payment.completed", "payment.failed"), 60001);
        Map<String, Object> duplicateEvents = request(simulatorUrl(), Map.of(),
                List.of("payment.completed", "payment.completed"), 10000);
        Map<String, Object> invalidEvents = request(simulatorUrl(), Map.of(),
                List.of("payment.completed", "invalid event"), 10000);
        Map<String, Object> badQuery = new HashMap<>(request(simulatorUrl(), Map.of(),
                List.of("payment.completed"), 10000));
        @SuppressWarnings("unchecked")
        Map<String, Object> destination = new HashMap<>((Map<String, Object>) badQuery.get("destination"));
        destination.put("query_params", Map.of("bad\nname", "value"));
        badQuery.put("destination", destination);

        for (Map<String, Object> body : List.of(badHeader, badTimeout, duplicateEvents,
                invalidEvents, badQuery)) {
            assertThat(register(credentials.key(), body).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        }
        assertThat(count("SELECT count(*) FROM endpoints WHERE organization_id = ?",
                credentials.organizationId())).isZero();
    }

    @Test
    void encryptsSupportedAuthenticationAndReturnsOnlyRedactedConfiguration() throws Exception {
        Credentials credentials = credentials();
        List<Map<String, Object>> configurations = List.of(
                Map.of("type", "STATIC_HEADER", "header_name", "X-Api-Key", "value", "static-secret"),
                Map.of("type", "STATIC_API_KEY", "header_name", "X-Api-Key", "value", "static-secret"),
                Map.of("type", "BEARER", "token", "bearer-secret"),
                Map.of("type", "BASIC", "username", "customer", "password", "basic-secret"),
                Map.of("type", "CLIENT_CREDENTIALS", "token_url", simulatorUrl() + "/token",
                        "client_id", "client", "client_secret", "oauth-secret",
                        "token_response", Map.of("access_token_path", "$.access_token",
                                "token_type", "Bearer")),
                Map.of("type", "TOKEN_EXCHANGE", "token_request", Map.of(
                                "method", "POST", "url", simulatorUrl() + "/token",
                                "headers", Map.of("Content-Type", "application/json"),
                                "body_fields", Map.of("username", Map.of("source", "secret", "name", "username"))),
                        "token_response", Map.of("access_token_path", "$.data.accessToken",
                                "token_type", "Bearer"),
                        "token_placement", Map.of("type", "HEADER", "name", "Authorization",
                                "prefix", "Bearer "), "secrets", Map.of("username", "exchange-secret")));
        for (Map<String, Object> configuration : configurations) {
            Map<String, Object> body = new HashMap<>(request(simulatorUrl()));
            body.put("authentication", configuration);
            ResponseEntity<JsonNode> created = register(credentials.key(), body);
            assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
            String id = created.getBody().path("data").path("endpoint_id").asText();
            ResponseEntity<JsonNode> read = readEndpoint(credentials.key(), id);
            assertThat(read.getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(read.getBody().path("data").path("authentication").path("type").asText())
                    .isEqualTo(configuration.get("type"));
            assertThat(read.getBody().toString()).doesNotContain("secret", "ciphertext", "wrapped_dek");
            try (var connection = DriverManager.getConnection(
                    DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword());
                    var statement = connection.prepareStatement("""
                            SELECT auth_type, auth_config_ciphertext, auth_wrapped_dek, auth_nonce
                            FROM endpoints WHERE id = ? AND organization_id = ?
                            """)) {
                statement.setObject(1, UUID.fromString(id));
                statement.setObject(2, credentials.organizationId());
                try (var row = statement.executeQuery()) {
                    assertThat(row.next()).isTrue();
                    assertThat(row.getString(1)).isEqualTo(configuration.get("type"));
                    assertThat(row.getBytes(2)).hasSizeGreaterThan(16);
                    assertThat(row.getBytes(3)).hasSizeGreaterThan(16);
                    assertThat(row.getBytes(4)).hasSize(12);
                    assertThat(new String(row.getBytes(2), StandardCharsets.ISO_8859_1))
                            .doesNotContain("secret");
                }
            }
        }
    }

    @Test
    void rejectsConflictingStaticHeaderAndNeverStoresSecret() throws Exception {
        Credentials credentials = credentials();
        Map<String, Object> body = new HashMap<>(request(simulatorUrl(), Map.of("X-Api-Key", "public"),
                List.of("payment.completed"), 10000));
        body.put("authentication", Map.of("type", "STATIC_HEADER", "header_name", "X-Api-Key",
                "value", "private-secret"));

        ResponseEntity<JsonNode> rejected = register(credentials.key(), body);
        assertThat(rejected.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(rejected.getBody().toString()).doesNotContain("private-secret");
        assertThat(count("SELECT count(*) FROM endpoints WHERE organization_id = ?",
                credentials.organizationId())).isZero();
    }

    @Test
    void listsEndpointsAndSubscriptionsWithTenantScopedCursorPages() throws Exception {
        Credentials owner = credentials();
        Credentials other = credentials();
        String firstId = register(owner.key(), request(simulatorUrl())).getBody()
                .path("data").path("endpoint_id").asText();
        register(owner.key(), request(simulatorUrl()));
        register(other.key(), request(simulatorUrl()));

        ResponseEntity<JsonNode> firstPage = get(owner.key(), "/v1/endpoints?limit=1");
        assertThat(firstPage.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(firstPage.getBody().path("data").size()).isEqualTo(1);
        String cursor = firstPage.getBody().path("next_cursor").asText();
        assertThat(cursor).isNotBlank();
        ResponseEntity<JsonNode> nextPage = get(owner.key(), "/v1/endpoints?limit=1&cursor=" + cursor);
        assertThat(nextPage.getBody().path("data").size()).isEqualTo(1);
        assertThat(nextPage.getBody().path("next_cursor").isNull()).isTrue();
        assertThat(nextPage.getBody().toString()).doesNotContain(other.organizationId().toString());

        ResponseEntity<JsonNode> subscriptions = get(owner.key(), "/v1/subscriptions?limit=2");
        assertThat(subscriptions.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(subscriptions.getBody().path("data").size()).isEqualTo(2);
        assertThat(subscriptions.getBody().path("next_cursor").isNull()).isFalse();
        assertThat(get(owner.key(), "/v1/endpoints/" + firstId).getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(get(other.key(), "/v1/endpoints/" + firstId).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(get(owner.key(), "/v1/endpoints?limit=101").getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(get(owner.key(), "/v1/endpoints?cursor=invalid").getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void patchesWithVersionPreconditionAndWritesMonotonicSecretFreeChange() throws Exception {
        client.getRestTemplate().setRequestFactory(new JdkClientHttpRequestFactory());
        Credentials owner = credentials();
        String id = register(owner.key(), request(simulatorUrl())).getBody()
                .path("data").path("endpoint_id").asText();
        assertThat(count("SELECT config_revision FROM organizations WHERE id = ?",
                owner.organizationId())).isEqualTo(1);
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(owner.key());
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("If-Match", "\"1\"");
        ResponseEntity<JsonNode> patched = client.exchange("/v1/endpoints/" + id, HttpMethod.PATCH,
                new HttpEntity<>(Map.of("name", "Updated", "status", "DISABLED"), headers), JsonNode.class);
        assertThat(patched.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(patched.getBody().path("data").path("version").asLong()).isEqualTo(2);
        assertThat(patched.getBody().path("data").path("status").asText()).isEqualTo("DISABLED");
        assertThat(patched.getHeaders().getETag()).isEqualTo("\"2\"");
        assertThat(count("SELECT config_revision FROM organizations WHERE id = ?",
                owner.organizationId())).isEqualTo(2);
        assertThat(count("SELECT count(*) FROM control_outbox WHERE organization_id = ?",
                owner.organizationId())).isEqualTo(2);
        assertThat(count("SELECT count(*) FROM audit_records WHERE organization_id = ? AND action = 'endpoint.disabled'",
                owner.organizationId())).isEqualTo(1);

        ResponseEntity<JsonNode> stale = client.exchange("/v1/endpoints/" + id, HttpMethod.PATCH,
                new HttpEntity<>(Map.of("name", "Stale"), headers), JsonNode.class);
        assertThat(stale.getStatusCode()).isEqualTo(HttpStatus.PRECONDITION_FAILED);
        assertThat(get(owner.key(), "/v1/endpoints/" + id).getBody().path("data").path("name").asText())
                .isEqualTo("Updated");
        assertThat(count("SELECT config_revision FROM organizations WHERE id = ?",
                owner.organizationId())).isEqualTo(2);
    }

    @Test
    void rotatesSigningSecretOnceAndNeverLeaksItToReadOrOutbox() throws Exception {
        Credentials owner = credentials();
        String id = register(owner.key(), request(simulatorUrl())).getBody()
                .path("data").path("endpoint_id").asText();
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(owner.key());
        headers.set("If-Match", "\"1\"");
        ResponseEntity<JsonNode> rotated = client.exchange("/v1/endpoints/" + id + "/rotate-secret",
                HttpMethod.POST, new HttpEntity<>(null, headers), JsonNode.class);
        assertThat(rotated.getStatusCode()).isEqualTo(HttpStatus.OK);
        String secret = rotated.getBody().path("data").path("signing_secret").asText();
        assertThat(secret).hasSizeGreaterThan(32);
        assertThat(rotated.getBody().path("data").path("version").asLong()).isEqualTo(2);
        assertThat(get(owner.key(), "/v1/endpoints/" + id).getBody().toString()).doesNotContain(secret);
        assertThat(count("SELECT count(*) FROM endpoints WHERE id = ? AND signing_secret_ciphertext IS NOT NULL",
                UUID.fromString(id))).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM control_outbox WHERE organization_id = ? AND payload::text LIKE ?",
                owner.organizationId(), "%" + secret + "%")).isZero();
        assertThat(count("SELECT count(*) FROM audit_records WHERE organization_id = ? AND action = 'endpoint.secret_rotated'",
                owner.organizationId())).isEqualTo(1);
        assertThat(client.exchange("/v1/endpoints/" + id + "/rotate-secret", HttpMethod.POST,
                new HttpEntity<>(null, headers), JsonNode.class).getStatusCode())
                .isEqualTo(HttpStatus.PRECONDITION_FAILED);
    }

    @Test
    void preservesCredentialAadVersionsAcrossUnrelatedUpdates() throws Exception {
        client.getRestTemplate().setRequestFactory(new JdkClientHttpRequestFactory());
        Credentials owner = credentials();
        Map<String, Object> body = new HashMap<>(request(simulatorUrl()));
        body.put("authentication", Map.of("type", "BEARER", "token", "first-token"));
        String id = register(owner.key(), body).getBody().path("data").path("endpoint_id").asText();

        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(owner.key());
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("If-Match", "\"1\"");
        ResponseEntity<JsonNode> renamed = client.exchange("/v1/endpoints/" + id, HttpMethod.PATCH,
                new HttpEntity<>(Map.of("name", "New name"), headers), JsonNode.class);
        assertThat(renamed.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(decryptStored(owner.organizationId(), UUID.fromString(id), "auth_config"))
                .contains("first-token");
        assertThat(count("SELECT auth_config_version FROM endpoints WHERE id = ?", UUID.fromString(id)))
                .isEqualTo(1);
        assertThat(count("SELECT auth_credential_version FROM endpoints WHERE id = ?", UUID.fromString(id)))
                .isEqualTo(1);

        headers.set("If-Match", "\"2\"");
        ResponseEntity<JsonNode> replaced = client.exchange("/v1/endpoints/" + id, HttpMethod.PATCH,
                new HttpEntity<>(Map.of("authentication", Map.of("type", "BEARER", "token", "second-token")),
                        headers), JsonNode.class);
        assertThat(replaced.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(decryptStored(owner.organizationId(), UUID.fromString(id), "auth_config"))
                .contains("second-token").doesNotContain("first-token");
        assertThat(count("SELECT auth_config_version FROM endpoints WHERE id = ?", UUID.fromString(id)))
                .isEqualTo(3);
        assertThat(count("SELECT auth_credential_version FROM endpoints WHERE id = ?", UUID.fromString(id)))
                .isEqualTo(2);

        headers.set("If-Match", "\"3\"");
        ResponseEntity<JsonNode> rotated = client.exchange("/v1/endpoints/" + id + "/rotate-secret",
                HttpMethod.POST, new HttpEntity<>(null, headers), JsonNode.class);
        assertThat(rotated.getStatusCode()).isEqualTo(HttpStatus.OK);
        String signingSecret = rotated.getBody().path("data").path("signing_secret").asText();
        headers.set("If-Match", "\"4\"");
        assertThat(client.exchange("/v1/endpoints/" + id, HttpMethod.PATCH,
                new HttpEntity<>(Map.of("status", "PAUSED"), headers), JsonNode.class).getStatusCode())
                .isEqualTo(HttpStatus.OK);
        assertThat(decryptStored(owner.organizationId(), UUID.fromString(id), "signing_secret"))
                .isEqualTo(signingSecret);
        assertThat(count("SELECT signing_secret_version FROM endpoints WHERE id = ?", UUID.fromString(id)))
                .isEqualTo(4);
    }

    @Test
    void rejectsPatchThatCollidesWithStoredAuthenticationHeader() throws Exception {
        client.getRestTemplate().setRequestFactory(new JdkClientHttpRequestFactory());
        Credentials owner = credentials();
        Map<String, Object> body = new HashMap<>(request(simulatorUrl()));
        body.put("authentication", Map.of("type", "STATIC_HEADER", "header_name", "X-Api-Key",
                "value", "private-token"));
        String id = register(owner.key(), body).getBody().path("data").path("endpoint_id").asText();
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(owner.key());
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("If-Match", "\"1\"");
        Map<String, Object> destination = Map.of("base_url", simulatorUrl(), "path", "/hook",
                "query_params", Map.of(), "headers", Map.of("x-api-key", "public"),
                "metadata", Map.of());
        ResponseEntity<JsonNode> rejected = client.exchange("/v1/endpoints/" + id, HttpMethod.PATCH,
                new HttpEntity<>(Map.of("destination", destination), headers), JsonNode.class);
        assertThat(rejected.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(rejected.getBody().toString()).doesNotContain("private-token");
        assertThat(count("SELECT version FROM endpoints WHERE id = ?", UUID.fromString(id))).isEqualTo(1);
    }

    private Credentials credentials() {
        HttpHeaders admin = new HttpHeaders();
        admin.setContentType(MediaType.APPLICATION_JSON);
        admin.set("X-Provisioning-Credential", PROVISIONING_CREDENTIAL);
        admin.set("Idempotency-Key", UUID.randomUUID().toString());
        ResponseEntity<JsonNode> organization = client.exchange("/v1/organizations", HttpMethod.POST,
                new HttpEntity<>(Map.of("name", "Endpoint Owner"), admin), JsonNode.class);
        assertThat(organization.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String firstKey = organization.getBody().path("data").path("api_key").asText();
        String keyId = firstKey.split("_")[2];
        ResponseEntity<JsonNode> rotated = client.exchange("/v1/api-keys/" + keyId + "/rotate", HttpMethod.POST,
                new HttpEntity<>(Map.of("scopes", List.of("endpoints:write", "endpoints:read")), admin), JsonNode.class);
        assertThat(rotated.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        return new Credentials(UUID.fromString(organization.getBody().path("data").path("organization_id").asText()),
                rotated.getBody().path("data").path("api_key").asText());
    }

    private ResponseEntity<JsonNode> register(String key, Map<String, Object> body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(key);
        return client.exchange("/v1/endpoints", HttpMethod.POST, new HttpEntity<>(body, headers), JsonNode.class);
    }

    private ResponseEntity<JsonNode> readEndpoint(String key, String id) {
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(key);
        return client.exchange("/v1/endpoints/" + id, HttpMethod.GET,
                new HttpEntity<>(headers), JsonNode.class);
    }

    private ResponseEntity<JsonNode> get(String key, String path) {
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(key);
        return client.exchange(path, HttpMethod.GET, new HttpEntity<>(headers), JsonNode.class);
    }

    private Map<String, Object> request(String baseUrl) {
        return request(baseUrl, Map.of("Content-Type", "application/json"),
                List.of("payment.completed", "payment.failed"), 10000);
    }

    private Map<String, Object> request(String baseUrl, Map<String, String> headers,
            List<String> eventTypes, int timeoutMs) {
        return Map.of("name", "Payments", "destination", Map.of(
                        "base_url", baseUrl, "path", "/hook", "query_params", Map.of("source", "test"),
                        "headers", headers, "metadata", Map.of("environment", "test")),
                "authentication", Map.of("type", "NONE"),
                "event_types", eventTypes,
                "delivery_policy", Map.of("timeout_ms", timeoutMs, "max_attempts", 3, "max_concurrency", 2));
    }

    private String simulatorUrl() {
        return "http://localhost:" + SIMULATOR.getAddress().getPort();
    }

    private long count(String sql, Object... values) throws Exception {
        try (var connection = DriverManager.getConnection(
                DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword());
                var statement = connection.prepareStatement(sql)) {
            for (int index = 0; index < values.length; index++) {
                statement.setObject(index + 1, values[index]);
            }
            try (var rows = statement.executeQuery()) {
                rows.next();
                return rows.getLong(1);
            }
        }
    }

    private String decryptStored(UUID organizationId, UUID endpointId, String field) throws Exception {
        String prefix = field.equals("auth_config") ? "auth" : "signing";
        String versionColumn = field.equals("auth_config") ? "auth_config_version" : "signing_secret_version";
        String sql = "SELECT " + prefix + "_secret_ciphertext";
        if (field.equals("auth_config")) {
            sql = "SELECT auth_config_ciphertext";
        }
        sql += ", " + prefix + "_wrapped_dek, " + prefix + "_key_version, "
                + prefix + "_nonce, " + versionColumn + " FROM endpoints WHERE id = ? AND organization_id = ?";
        try (var connection = DriverManager.getConnection(
                DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword());
                var statement = connection.prepareStatement(sql)) {
            statement.setObject(1, endpointId);
            statement.setObject(2, organizationId);
            try (var row = statement.executeQuery()) {
                assertThat(row.next()).isTrue();
                EncryptedSecret encrypted = new EncryptedSecret(row.getBytes(1), row.getBytes(2),
                        row.getString(3), row.getBytes(4));
                return new String(secretCipher.decrypt(organizationId, endpointId, field,
                        row.getLong(5), encrypted), StandardCharsets.UTF_8);
            }
        }
    }

    private record Credentials(UUID organizationId, String key) {
        @Override
        public String toString() {
            return "Credentials[organizationId=" + organizationId + ", key=[REDACTED]]";
        }
    }
}

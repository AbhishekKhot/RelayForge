package com.relayforge.controlplane.endpoint.auth;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;

public final class EndpointKeyCheckService {
    private static final byte[] EXPECTED = checkValue();
    private final JdbcTemplate jdbc;
    private final KeyEncryptionService wrapping;

    public EndpointKeyCheckService(JdbcTemplate jdbc, KeyEncryptionService wrapping) {
        this.jdbc = jdbc;
        this.wrapping = wrapping;
    }

    public void verifyOnStartup() {
        KeyEncryptionService.WrappedKey active = wrapping.wrap(EXPECTED);
        jdbc.update("""
                INSERT INTO encryption_key_checks (key_version, wrapped_check) VALUES (?, ?)
                ON CONFLICT (key_version) DO NOTHING
                """, active.version(), active.bytes());
        List<KeyEncryptionService.WrappedKey> checks = jdbc.query("""
                SELECT key_version, wrapped_check FROM encryption_key_checks
                """, (row, index) -> new KeyEncryptionService.WrappedKey(
                row.getBytes("wrapped_check"), row.getString("key_version")));
        for (KeyEncryptionService.WrappedKey check : checks) {
            byte[] actual = wrapping.unwrap(check);
            try {
                if (!MessageDigest.isEqual(EXPECTED, actual)) {
                    throw new IllegalStateException("Endpoint encryption key verification failed");
                }
            } finally {
                Arrays.fill(actual, (byte) 0);
            }
        }
    }

    private static byte[] checkValue() {
        try {
            return MessageDigest.getInstance("SHA-256")
                    .digest("relayforge-endpoint-key-check-v1".getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("SHA-256 is unavailable", unavailable);
        }
    }
}

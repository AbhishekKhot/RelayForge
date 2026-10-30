package com.relayforge.controlplane.endpoint.auth;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public final class LocalKeyEncryptionService implements KeyEncryptionService {
    private static final int NONCE_BYTES = 12;
    private static final int TAG_BITS = 128;
    private static final byte[] WRAP_AAD = "relayforge-dek-wrap-v1".getBytes(StandardCharsets.UTF_8);

    private final Map<String, byte[]> keys;
    private final String activeVersion;
    private final SecureRandom random;

    private LocalKeyEncryptionService(Map<String, byte[]> keys, String activeVersion, SecureRandom random) {
        this.keys = Map.copyOf(keys);
        this.activeVersion = activeVersion;
        this.random = random;
    }

    public static LocalKeyEncryptionService fromSettings(String entries, String activeVersion, boolean localMode) {
        if (!localMode || entries == null || entries.isBlank() || activeVersion == null) {
            throw new IllegalStateException("Local encryption key configuration is unavailable");
        }
        Map<String, byte[]> keys = new HashMap<>();
        for (String entry : entries.split(",", -1)) {
            String[] parts = entry.split(":", -1);
            if (parts.length != 2 || !parts[0].matches("[A-Za-z0-9_-]{1,80}")
                    || !parts[1].matches("[0-9a-fA-F]{64}")) {
                throw new IllegalStateException("Invalid local encryption key configuration");
            }
            if (keys.putIfAbsent(parts[0], HexFormat.of().parseHex(parts[1])) != null) {
                throw new IllegalStateException("Duplicate local encryption key version");
            }
        }
        if (!keys.containsKey(activeVersion)) {
            throw new IllegalStateException("Active encryption key version is unavailable");
        }
        return new LocalKeyEncryptionService(keys, activeVersion, new SecureRandom());
    }

    @Override
    public String activeVersion() {
        return activeVersion;
    }

    @Override
    public WrappedKey wrap(byte[] dataKey) {
        if (dataKey == null || dataKey.length != 32) {
            throw new IllegalArgumentException("Invalid data key");
        }
        byte[] nonce = new byte[NONCE_BYTES];
        random.nextBytes(nonce);
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(keys.get(activeVersion), "AES"),
                    new GCMParameterSpec(TAG_BITS, nonce));
            cipher.updateAAD(WRAP_AAD);
            byte[] encrypted = cipher.doFinal(dataKey);
            return new WrappedKey(ByteBuffer.allocate(NONCE_BYTES + encrypted.length)
                    .put(nonce).put(encrypted).array(), activeVersion);
        } catch (GeneralSecurityException unavailable) {
            throw new IllegalStateException("Data key wrapping failed", unavailable);
        }
    }

    @Override
    public byte[] unwrap(WrappedKey wrapped) {
        if (wrapped == null || !keys.containsKey(wrapped.version())
                || wrapped.bytes().length != NONCE_BYTES + 32 + TAG_BITS / 8) {
            throw new IllegalStateException("Data key is unavailable");
        }
        try {
            byte[] bytes = wrapped.bytes();
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(keys.get(wrapped.version()), "AES"),
                    new GCMParameterSpec(TAG_BITS, bytes, 0, NONCE_BYTES));
            cipher.updateAAD(WRAP_AAD);
            return cipher.doFinal(bytes, NONCE_BYTES, bytes.length - NONCE_BYTES);
        } catch (GeneralSecurityException invalid) {
            throw new IllegalStateException("Data key is unavailable");
        }
    }
}

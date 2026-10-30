package com.relayforge.ingestion.event;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

public final class IdempotencyBucket {
    private IdempotencyBucket() {
    }

    public static int forKey(UUID organizationId, String key) {
        validate(key);
        if (organizationId == null) {
            throw new IllegalArgumentException("Missing organization");
        }
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        ByteBuffer input = ByteBuffer.allocate(18 + keyBytes.length);
        input.putLong(organizationId.getMostSignificantBits());
        input.putLong(organizationId.getLeastSignificantBits());
        input.putShort((short) keyBytes.length);
        input.put(keyBytes);
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(input.array());
            return ((digest[0] & 0xff) << 4) | ((digest[1] & 0xf0) >>> 4);
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("SHA-256 is unavailable", unavailable);
        }
    }

    public static void validate(String key) {
        if (key == null || key.isEmpty() || key.getBytes(StandardCharsets.UTF_8).length > 255
                || Character.isWhitespace(key.charAt(0)) || Character.isWhitespace(key.charAt(key.length() - 1))
                || key.codePoints().anyMatch(value -> Character.isISOControl(value))) {
            throw new IllegalArgumentException("Invalid idempotency key");
        }
    }
}

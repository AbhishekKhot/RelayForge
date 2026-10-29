package com.relayforge.controlplane.security;

import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public final class ApiKeyCodec {
    private static final Pattern KEY_PATTERN = Pattern.compile("rf_live_([0-9a-f]{32})_([A-Za-z0-9_-]{43})");
    private final SecureRandom random;
    private final SecretKeySpec hmacKey;

    public ApiKeyCodec(SecureRandom random, byte[] hmacKey) {
        if (hmacKey.length != 32) {
            throw new IllegalArgumentException("API key HMAC key must contain 32 bytes");
        }
        this.random = random;
        this.hmacKey = new SecretKeySpec(hmacKey.clone(), "HmacSHA256");
    }

    public IssuedKey issue() {
        byte[] identifier = new byte[16];
        byte[] secret = new byte[32];
        random.nextBytes(identifier);
        random.nextBytes(secret);
        String keyId = HexFormat.of().formatHex(identifier);
        String plaintext = "rf_live_" + keyId + "_" + Base64.getUrlEncoder().withoutPadding().encodeToString(secret);
        return new IssuedKey(keyId, plaintext, digest(secret));
    }

    public Optional<String> keyId(String plaintext) {
        Matcher matcher = KEY_PATTERN.matcher(plaintext == null ? "" : plaintext);
        return matcher.matches() ? Optional.of(matcher.group(1)) : Optional.empty();
    }

    public boolean verify(String plaintext, byte[] storedDigest) {
        Matcher matcher = KEY_PATTERN.matcher(plaintext == null ? "" : plaintext);
        if (!matcher.matches() || storedDigest == null || storedDigest.length != 32) {
            return false;
        }
        try {
            byte[] secret = Base64.getUrlDecoder().decode(matcher.group(2));
            return secret.length == 32 && MessageDigest.isEqual(storedDigest, digest(secret));
        } catch (IllegalArgumentException malformed) {
            return false;
        }
    }

    private byte[] digest(byte[] secret) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(hmacKey);
            return mac.doFinal(secret);
        } catch (java.security.GeneralSecurityException unavailable) {
            throw new IllegalStateException("HMAC-SHA256 is unavailable", unavailable);
        }
    }

    public static final class IssuedKey {
        private final String keyId;
        private final String plaintext;
        private final byte[] digest;

        private IssuedKey(String keyId, String plaintext, byte[] digest) {
            this.keyId = keyId;
            this.plaintext = plaintext;
            this.digest = digest.clone();
        }

        public String keyId() {
            return keyId;
        }

        public String plaintext() {
            return plaintext;
        }

        public byte[] digest() {
            return digest.clone();
        }

        @Override
        public String toString() {
            return "IssuedKey[keyId=" + keyId + ", plaintext=[REDACTED]]";
        }
    }
}

package com.relayforge.controlplane.endpoint.auth;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.UUID;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public final class EndpointSecretCipher {
    private static final int NONCE_BYTES = 12;
    private static final int TAG_BITS = 128;
    private final KeyEncryptionService wrapping;
    private final SecureRandom random;

    public EndpointSecretCipher(KeyEncryptionService wrapping, SecureRandom random) {
        this.wrapping = wrapping;
        this.random = random;
    }

    public EncryptedSecret encrypt(UUID organizationId, UUID endpointId, String field,
            long version, byte[] plaintext) {
        if (organizationId == null || endpointId == null || plaintext == null || plaintext.length == 0) {
            throw new IllegalArgumentException("Invalid secret context");
        }
        byte[] dataKey = new byte[32];
        byte[] nonce = new byte[NONCE_BYTES];
        random.nextBytes(dataKey);
        random.nextBytes(nonce);
        try {
            Cipher cipher = initialized(Cipher.ENCRYPT_MODE, dataKey, nonce,
                    associatedData(organizationId, endpointId, field, version));
            byte[] ciphertext = cipher.doFinal(plaintext);
            KeyEncryptionService.WrappedKey wrapped = wrapping.wrap(dataKey);
            return new EncryptedSecret(ciphertext, wrapped.bytes(), wrapped.version(), nonce);
        } catch (GeneralSecurityException unavailable) {
            throw new IllegalStateException("Secret encryption failed", unavailable);
        } finally {
            Arrays.fill(dataKey, (byte) 0);
        }
    }

    public byte[] decrypt(UUID organizationId, UUID endpointId, String field,
            long version, EncryptedSecret encrypted) {
        byte[] dataKey = wrapping.unwrap(new KeyEncryptionService.WrappedKey(
                encrypted.wrappedDek(), encrypted.keyVersion()));
        try {
            Cipher cipher = initialized(Cipher.DECRYPT_MODE, dataKey, encrypted.nonce(),
                    associatedData(organizationId, endpointId, field, version));
            return cipher.doFinal(encrypted.ciphertext());
        } catch (GeneralSecurityException invalid) {
            throw new IllegalStateException("Secret could not be decrypted");
        } finally {
            Arrays.fill(dataKey, (byte) 0);
        }
    }

    public EncryptedSecret rewrap(UUID organizationId, UUID endpointId, String field,
            long version, EncryptedSecret encrypted) {
        byte[] dataKey = wrapping.unwrap(new KeyEncryptionService.WrappedKey(
                encrypted.wrappedDek(), encrypted.keyVersion()));
        try {
            Cipher verifier = initialized(Cipher.DECRYPT_MODE, dataKey, encrypted.nonce(),
                    associatedData(organizationId, endpointId, field, version));
            byte[] plaintext = verifier.doFinal(encrypted.ciphertext());
            Arrays.fill(plaintext, (byte) 0);
            KeyEncryptionService.WrappedKey wrapped = wrapping.wrap(dataKey);
            return new EncryptedSecret(encrypted.ciphertext(), wrapped.bytes(), wrapped.version(),
                    encrypted.nonce());
        } catch (GeneralSecurityException invalid) {
            throw new IllegalStateException("Secret could not be rewrapped");
        } finally {
            Arrays.fill(dataKey, (byte) 0);
        }
    }

    private Cipher initialized(int mode, byte[] key, byte[] nonce, byte[] aad)
            throws GeneralSecurityException {
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(mode, new SecretKeySpec(key, "AES"), new GCMParameterSpec(TAG_BITS, nonce));
        cipher.updateAAD(aad);
        return cipher;
    }

    private byte[] associatedData(UUID organizationId, UUID endpointId, String field, long version) {
        if (organizationId == null || endpointId == null || field == null || !field.matches("[a-z_]{1,40}")
                || version < 1) {
            throw new IllegalArgumentException("Invalid secret context");
        }
        return (organizationId + ":" + endpointId + ":" + field + ":" + version)
                .getBytes(StandardCharsets.UTF_8);
    }

    public record EncryptedSecret(byte[] ciphertext, byte[] wrappedDek, String keyVersion, byte[] nonce) {
        public EncryptedSecret {
            ciphertext = ciphertext.clone();
            wrappedDek = wrappedDek.clone();
            nonce = nonce.clone();
        }

        @Override
        public byte[] ciphertext() {
            return ciphertext.clone();
        }

        @Override
        public byte[] wrappedDek() {
            return wrappedDek.clone();
        }

        @Override
        public byte[] nonce() {
            return nonce.clone();
        }

        @Override
        public String toString() {
            return "EncryptedSecret[ciphertext=[REDACTED], wrappedDek=[REDACTED], keyVersion="
                    + keyVersion + ", nonce=[REDACTED]]";
        }
    }
}

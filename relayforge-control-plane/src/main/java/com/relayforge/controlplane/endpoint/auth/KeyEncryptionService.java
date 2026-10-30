package com.relayforge.controlplane.endpoint.auth;

public interface KeyEncryptionService {
    String activeVersion();

    WrappedKey wrap(byte[] dataKey);

    byte[] unwrap(WrappedKey key);

    record WrappedKey(byte[] bytes, String version) {
        public WrappedKey {
            bytes = bytes.clone();
        }

        @Override
        public byte[] bytes() {
            return bytes.clone();
        }

        @Override
        public String toString() {
            return "WrappedKey[bytes=[REDACTED], version=" + version + "]";
        }
    }
}

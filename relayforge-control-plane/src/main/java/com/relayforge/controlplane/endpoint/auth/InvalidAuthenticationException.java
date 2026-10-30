package com.relayforge.controlplane.endpoint.auth;

public final class InvalidAuthenticationException extends RuntimeException {
    public InvalidAuthenticationException() {
        super("Invalid authentication configuration");
    }
}

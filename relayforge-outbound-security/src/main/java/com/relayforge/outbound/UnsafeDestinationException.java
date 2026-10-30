package com.relayforge.outbound;

public final class UnsafeDestinationException extends RuntimeException {
    public UnsafeDestinationException() {
        super("Endpoint destination is not permitted");
    }
}

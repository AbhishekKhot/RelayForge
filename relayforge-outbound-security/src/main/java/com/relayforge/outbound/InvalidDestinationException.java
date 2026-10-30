package com.relayforge.outbound;

public final class InvalidDestinationException extends RuntimeException {
    public InvalidDestinationException() {
        super("Invalid endpoint destination");
    }
}

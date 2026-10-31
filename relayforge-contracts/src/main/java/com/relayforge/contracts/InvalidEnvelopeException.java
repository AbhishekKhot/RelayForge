package com.relayforge.contracts;

public final class InvalidEnvelopeException extends RuntimeException {
    private final String reasonCode;

    public InvalidEnvelopeException(String reasonCode) {
        super("Invalid message envelope: " + reasonCode);
        this.reasonCode = reasonCode;
    }

    public String reasonCode() {
        return reasonCode;
    }
}

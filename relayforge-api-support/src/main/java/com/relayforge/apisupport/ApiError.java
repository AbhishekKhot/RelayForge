package com.relayforge.apisupport;

public record ApiError(String code, String message, String requestId) {
}

package com.relayforge.outbound;

import java.net.URI;
import java.util.Map;

public final class ValidatedDestination {
    private final URI uri;
    private final Map<String, String> headers;

    ValidatedDestination(URI uri, Map<String, String> headers) {
        this.uri = uri;
        this.headers = Map.copyOf(headers);
    }

    public URI uri() {
        return uri;
    }

    public Map<String, String> headers() {
        return headers;
    }
}

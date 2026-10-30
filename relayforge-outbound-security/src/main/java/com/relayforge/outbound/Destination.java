package com.relayforge.outbound;

import java.util.Map;

public record Destination(String baseUrl, String path, Map<String, String> queryParams,
        Map<String, String> headers) {
}

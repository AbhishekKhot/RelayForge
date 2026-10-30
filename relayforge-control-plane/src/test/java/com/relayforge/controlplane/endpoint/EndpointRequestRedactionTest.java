package com.relayforge.controlplane.endpoint;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.controlplane.endpoint.EndpointController.DestinationRequest;
import com.relayforge.controlplane.endpoint.EndpointController.EndpointRequest;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EndpointRequestRedactionTest {
    @Test
    void doesNotRenderDestinationHeadersOrAuthentication() throws Exception {
        DestinationRequest destination = new DestinationRequest("https://customer.example", "/hook",
                Map.of("token", "query-secret"), Map.of("X-Test", "header-secret"),
                Map.of("note", "metadata-secret"));
        var authentication = new ObjectMapper().readTree("{\"type\":\"BEARER\",\"token\":\"auth-secret\"}");
        EndpointRequest request = new EndpointRequest("customer-secret", destination,
                authentication, List.of("payment.completed"), null);

        assertThat(destination.toString()).doesNotContain("query-secret", "header-secret", "metadata-secret");
        assertThat(request.toString()).doesNotContain("auth-secret", "customer-secret", "header-secret");
    }
}

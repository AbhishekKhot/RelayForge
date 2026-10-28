package com.relayforge.controlplane;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.RequestEntity;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ControlPlaneBootstrapTest {
    @Autowired
    private TestRestTemplate client;

    @Test
    void servesHealthWithRequestId() {
        var request = new RequestEntity<Void>(null, headers("bootstrap-1"), HttpMethod.GET, URI.create("/health/live"));
        var response = client.exchange(request, String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getHeaders().getFirst("X-Request-Id")).isEqualTo("bootstrap-1");
        assertThat(response.getBody()).contains("UP");
    }

    @Test
    void reportsReadyAfterStartup() {
        var response = client.getForEntity("/health/ready", String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("UP");
    }

    @Test
    void hidesUnknownRouteDetails() {
        var request = new RequestEntity<Void>(null, headers("missing-1"), HttpMethod.GET, URI.create("/missing"));
        var response = client.exchange(request, String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(response.getBody()).contains("NOT_FOUND", "missing-1");
        assertThat(response.getBody()).doesNotContain("Exception", "trace");
    }

    @Test
    void rejectsUnsupportedMethodWithoutInternalDetails() {
        var request = new RequestEntity<Void>(null, headers("method-1"), HttpMethod.DELETE, URI.create("/health/live"));
        var response = client.exchange(request, String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.METHOD_NOT_ALLOWED);
        assertThat(response.getBody()).contains("METHOD_NOT_ALLOWED", "method-1");
    }

    private HttpHeaders headers(String requestId) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Request-Id", requestId);
        return headers;
    }
}

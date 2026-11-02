package com.relayforge.controlplane.endpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

class ConfigurationSnapshotControllerTest {
    private static final String CREDENTIAL = "test-config-credential-01234567890123456789";
    private final ConfigurationSnapshotService service = mock(ConfigurationSnapshotService.class);
    private final ConfigurationSnapshotController controller =
            new ConfigurationSnapshotController(service, CREDENTIAL);

    @Test
    void requiresDedicatedCredential() {
        UUID organization = UUID.randomUUID();
        assertThatThrownBy(() -> controller.endpoints(organization, null, 10, null))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        failure -> assertThat(failure.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED));
        assertThatThrownBy(() -> controller.subscriptions(organization, null, 10, "wrong"))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        failure -> assertThat(failure.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED));
    }

    @Test
    void returnsOnlySecretFreeProjectionState() {
        UUID organization = UUID.randomUUID();
        UUID endpoint = UUID.randomUUID();
        var page = new ConfigurationSnapshotService.Page<>(2L,
                List.of(new ConfigurationSnapshotRepository.EndpointState(endpoint, "ACTIVE", 1L)), null);
        when(service.endpoints(organization, null, 10)).thenReturn(page);
        assertThat(controller.endpoints(organization, null, 10, CREDENTIAL)).isEqualTo(page);
    }
}

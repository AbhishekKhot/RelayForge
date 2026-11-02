package com.relayforge.controlplane.endpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

class ConfigurationSnapshotServiceTest {
    private final UUID organization = UUID.randomUUID();
    private final ConfigurationSnapshotRepository repository = mock(ConfigurationSnapshotRepository.class);
    private final ConfigurationSnapshotService service = new ConfigurationSnapshotService(repository);

    @Test
    void returnsBoundedPageAtStableRevision() {
        UUID first = UUID.fromString("00000000-0000-0000-0000-000000000001");
        UUID second = UUID.fromString("00000000-0000-0000-0000-000000000002");
        UUID third = UUID.fromString("00000000-0000-0000-0000-000000000003");
        when(repository.revision(organization)).thenReturn(Optional.of(4L));
        when(repository.endpoints(organization, null, 3)).thenReturn(List.of(
                new ConfigurationSnapshotRepository.EndpointState(first, "ACTIVE", 1),
                new ConfigurationSnapshotRepository.EndpointState(second, "PAUSED", 2),
                new ConfigurationSnapshotRepository.EndpointState(third, "ACTIVE", 1)));

        var page = service.endpoints(organization, null, 2);

        assertThat(page.sourceRevision()).isEqualTo(4);
        assertThat(page.data()).hasSize(2);
        assertThat(page.nextCursor()).isEqualTo(second);
    }

    @Test
    void rejectsRevisionChangeDuringPage() {
        when(repository.revision(organization)).thenReturn(Optional.of(1L), Optional.of(2L));
        when(repository.endpoints(organization, null, 2)).thenReturn(List.of());
        assertThatThrownBy(() -> service.endpoints(organization, null, 1))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        failure -> assertThat(failure.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE));
    }

    @Test
    void rejectsMissingOrganizationAndUnboundedPage() {
        when(repository.revision(organization)).thenReturn(Optional.empty());
        assertThatThrownBy(() -> service.subscriptions(organization, null, 10))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        failure -> assertThat(failure.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND));
        assertThatThrownBy(() -> service.endpoints(organization, null, 101))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        failure -> assertThat(failure.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST));
    }
}

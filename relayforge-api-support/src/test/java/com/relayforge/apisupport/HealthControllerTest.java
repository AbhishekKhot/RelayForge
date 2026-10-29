package com.relayforge.apisupport;

import org.junit.jupiter.api.Test;
import org.springframework.boot.availability.ApplicationAvailability;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.http.HttpStatus;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HealthControllerTest {
    @Test
    void unavailableDependencyMakesReadinessUnavailableWithoutLeakingDetails() {
        var availability = mock(ApplicationAvailability.class);
        when(availability.getReadinessState()).thenReturn(ReadinessState.ACCEPTING_TRAFFIC);
        var controller = new HealthController(availability, List.of(() -> {
            throw new IllegalStateException("private database address");
        }));

        var response = controller.ready();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(response.getBody()).containsEntry("status", "DOWN");
        assertThat(response.getBody().toString()).doesNotContain("private database address");
        assertThat(controller.live()).containsEntry("status", "UP");
    }

    @Test
    void readinessRecoversWhenDependencyRecovers() {
        var availability = mock(ApplicationAvailability.class);
        when(availability.getReadinessState()).thenReturn(ReadinessState.ACCEPTING_TRAFFIC);
        var healthy = new boolean[]{false};
        var controller = new HealthController(availability, List.of(() -> healthy[0]));

        assertThat(controller.ready().getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        healthy[0] = true;
        assertThat(controller.ready().getStatusCode()).isEqualTo(HttpStatus.OK);
    }
}

package com.relayforge.analyticsapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

class AnalyticsServiceTest {
    private static final String FROM = "2026-11-03T10:00:00Z";
    private static final String TO = "2026-11-03T11:00:00Z";

    @Test
    void passesAuthenticatedOrganizationAndEndpointToRepository() {
        UUID organization = UUID.randomUUID();
        UUID endpoint = UUID.randomUUID();
        AnalyticsService service = new AnalyticsService(new AnalyticsRepository() {
            @Override
            public Overview overview(UUID tenant, AnalyticsRange range) {
                assertThat(tenant).isEqualTo(organization);
                return new Overview(1, 0, 0, 0, 0, null);
            }

            @Override
            public TimeSeries timeseries(UUID tenant, AnalyticsRange range, UUID selectedEndpoint) {
                assertThat(tenant).isEqualTo(organization);
                assertThat(selectedEndpoint).isEqualTo(endpoint);
                return new TimeSeries(List.of(), null);
            }
        });
        assertThat(service.overview(organization, FROM, TO).eventsAccepted()).isEqualTo(1);
        assertThat(service.timeseries(organization, FROM, TO, "hour", endpoint.toString())
                .points()).isEmpty();
        assertThatThrownBy(() -> service.timeseries(organization, FROM, TO, "hour", "bad-id"))
                .isInstanceOfSatisfying(ResponseStatusException.class, failure ->
                        assertThat(failure.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST));
    }

    @Test
    void mapsClickHouseFailureToUnavailable() {
        AnalyticsService service = new AnalyticsService(new AnalyticsRepository() {
            @Override
            public Overview overview(UUID tenant, AnalyticsRange range) throws IOException {
                throw new IOException("ClickHouse unavailable");
            }

            @Override
            public TimeSeries timeseries(UUID tenant, AnalyticsRange range, UUID endpoint)
                    throws IOException {
                throw new IOException("ClickHouse unavailable");
            }
        });
        UUID organization = UUID.randomUUID();
        assertThatThrownBy(() -> service.overview(organization, FROM, TO))
                .isInstanceOf(AnalyticsUnavailableException.class);
        assertThatThrownBy(() -> service.timeseries(organization, FROM, TO, "hour", null))
                .isInstanceOf(AnalyticsUnavailableException.class);
    }
}

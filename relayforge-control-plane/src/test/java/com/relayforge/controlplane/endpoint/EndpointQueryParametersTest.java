package com.relayforge.controlplane.endpoint;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EndpointQueryParametersTest {
    @Test
    void acceptsBoundedPagesAndOpaqueUuidCursor() {
        UUID cursor = UUID.randomUUID();
        var page = EndpointQueryParameters.page("20", cursor.toString());
        assertThat(page.limit()).isEqualTo(20);
        assertThat(page.cursor()).isEqualTo(cursor);
        assertThat(EndpointQueryParameters.page(null, null).limit()).isEqualTo(50);
    }

    @Test
    void rejectsUnboundedPagesAndMalformedCursor() {
        for (String limit : new String[]{"0", "101", "-1", "many"}) {
            assertThatThrownBy(() -> EndpointQueryParameters.page(limit, null))
                    .isInstanceOf(ResponseStatusException.class);
        }
        assertThatThrownBy(() -> EndpointQueryParameters.page("10", "bad"))
                .isInstanceOf(ResponseStatusException.class);
    }

    @Test
    void requiresStrongQuotedVersionPrecondition() {
        assertThat(EndpointQueryParameters.version("\"42\"")).isEqualTo(42);
        for (String value : new String[]{"42", "W/\"42\"", "\"0\"", "\"abc\""}) {
            assertThatThrownBy(() -> EndpointQueryParameters.version(value))
                    .isInstanceOf(ResponseStatusException.class);
        }
        assertThatThrownBy(() -> EndpointQueryParameters.version(null))
                .isInstanceOf(ResponseStatusException.class);
    }
}

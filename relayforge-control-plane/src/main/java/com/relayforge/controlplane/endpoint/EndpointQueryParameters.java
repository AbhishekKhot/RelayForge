package com.relayforge.controlplane.endpoint;

import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public final class EndpointQueryParameters {
    private EndpointQueryParameters() {
    }

    public static Page page(String limitText, String cursorText) {
        int limit = 50;
        try {
            if (limitText != null) {
                limit = Integer.parseInt(limitText);
            }
            if (limit < 1 || limit > 100) {
                throw invalid();
            }
            UUID cursor = cursorText == null ? null : UUID.fromString(cursorText);
            return new Page(limit, cursor);
        } catch (NumberFormatException invalidNumber) {
            throw invalid();
        } catch (IllegalArgumentException invalidCursor) {
            throw invalid();
        }
    }

    public static long version(String ifMatch) {
        if (ifMatch == null) {
            throw new ResponseStatusException(HttpStatus.PRECONDITION_REQUIRED);
        }
        if (!ifMatch.matches("\"[1-9][0-9]*\"")) {
            throw invalid();
        }
        try {
            return Long.parseLong(ifMatch.substring(1, ifMatch.length() - 1));
        } catch (NumberFormatException invalidNumber) {
            throw invalid();
        }
    }

    private static ResponseStatusException invalid() {
        return new ResponseStatusException(HttpStatus.BAD_REQUEST);
    }

    public record Page(int limit, UUID cursor) {
    }
}

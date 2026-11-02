package com.relayforge.deliveryapi.history;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.Base64;
import java.util.Set;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public final class DeliveryQueryParameters {
    private static final Set<String> STATUSES = Set.of("PENDING", "ENQUEUED", "IN_PROGRESS",
            "RETRY_SCHEDULED", "SUCCEEDED", "DEAD_LETTERED", "CANCELLED");
    private static final Duration MAX_WINDOW = Duration.ofDays(90);

    private DeliveryQueryParameters() {
    }

    public static Page parse(UUID organizationId, String status, String endpointId,
            String from, String to, String limit, String cursor, Clock clock) {
        if (organizationId == null || clock == null || status != null && !STATUSES.contains(status)) {
            throw invalid();
        }
        try {
            Instant now = clock.instant();
            DecodedCursor supplied = cursor == null ? null : decode(cursor);
            Instant upper = to == null ? supplied == null ? now : supplied.to() : Instant.parse(to);
            Instant lower = from == null ? supplied == null ? upper.minus(MAX_WINDOW) : supplied.from()
                    : Instant.parse(from);
            if (!lower.isBefore(upper) || Duration.between(lower, upper).compareTo(MAX_WINDOW) > 0
                    || upper.isAfter(now.plusSeconds(60))
                    || lower.isBefore(now.minus(MAX_WINDOW))) {
                throw invalid();
            }
            int size = limit == null ? 50 : Integer.parseInt(limit);
            if (size < 1 || size > 100) {
                throw invalid();
            }
            UUID endpoint = endpointId == null ? null : UUID.fromString(endpointId);
            if (endpointId != null && !endpoint.toString().equals(endpointId)) {
                throw invalid();
            }
            byte[] fingerprint = fingerprint(organizationId, status, endpoint, lower, upper);
            Cursor decoded = supplied == null ? null : checked(supplied, fingerprint, lower, upper);
            return new Page(status, endpoint, lower, upper, size, decoded, fingerprint);
        } catch (IllegalArgumentException | DateTimeException malformed) {
            throw invalid();
        }
    }

    public static String cursor(Page page, Instant createdAt, UUID deliveryId) {
        if (page == null || createdAt == null || deliveryId == null) {
            throw new IllegalArgumentException("Invalid delivery cursor");
        }
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(bytes);
            output.writeByte(1);
            writeInstant(output, page.from());
            writeInstant(output, page.to());
            writeInstant(output, createdAt);
            output.writeLong(deliveryId.getMostSignificantBits());
            output.writeLong(deliveryId.getLeastSignificantBits());
            output.write(page.fingerprint());
            return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException("Delivery cursor encoding failed", impossible);
        }
    }

    private static DecodedCursor decode(String encoded) {
        if (encoded.length() > 128 || !encoded.matches("[A-Za-z0-9_-]+")) {
            throw invalid();
        }
        try {
            byte[] bytes = Base64.getUrlDecoder().decode(encoded);
            if (bytes.length != 85) {
                throw invalid();
            }
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes));
            if (input.readUnsignedByte() != 1) {
                throw invalid();
            }
            Instant from = readInstant(input);
            Instant to = readInstant(input);
            Instant createdAt = readInstant(input);
            UUID id = new UUID(input.readLong(), input.readLong());
            byte[] supplied = input.readNBytes(32);
            return new DecodedCursor(from, to, createdAt, id, supplied);
        } catch (IOException | IllegalArgumentException | DateTimeException malformed) {
            throw invalid();
        }
    }

    private static Cursor checked(DecodedCursor value, byte[] fingerprint, Instant from, Instant to) {
        if (!MessageDigest.isEqual(fingerprint, value.fingerprint())
                || !value.from().equals(from) || !value.to().equals(to)
                || value.createdAt().isBefore(from) || !value.createdAt().isBefore(to)) {
            throw invalid();
        }
        return new Cursor(value.createdAt(), value.deliveryId());
    }

    private static void writeInstant(DataOutputStream output, Instant value) throws IOException {
        output.writeLong(value.getEpochSecond());
        output.writeInt(value.getNano());
    }

    private static Instant readInstant(DataInputStream input) throws IOException {
        return Instant.ofEpochSecond(input.readLong(), input.readInt());
    }

    private static byte[] fingerprint(UUID organizationId, String status, UUID endpoint,
            Instant from, Instant to) {
        try {
            String value = organizationId + "|" + (status == null ? "" : status) + "|"
                    + (endpoint == null ? "" : endpoint) + "|" + from + "|" + to;
            return MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("Delivery cursor hash is unavailable", unavailable);
        }
    }

    private static ResponseStatusException invalid() {
        return new ResponseStatusException(HttpStatus.BAD_REQUEST);
    }

    public record Page(String status, UUID endpointId, Instant from, Instant to,
            int limit, Cursor cursor, byte[] fingerprint) {
        public Page {
            fingerprint = fingerprint.clone();
        }

        @Override
        public byte[] fingerprint() {
            return fingerprint.clone();
        }
    }

    public record Cursor(Instant createdAt, UUID deliveryId) {
    }

    private record DecodedCursor(Instant from, Instant to, Instant createdAt,
            UUID deliveryId, byte[] fingerprint) {
    }
}

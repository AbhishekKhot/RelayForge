package com.relayforge.ingestion.event;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.util.Base64;
import java.util.UUID;

public final class EventLocator {
    private static final int VERSION = 1;

    private EventLocator() {
    }

    public static String encode(UUID eventId, LocalDate day, int bucketId, long mapEpoch,
            String ingestionShardId, String deliveryShardId) {
        if (eventId == null || day == null || bucketId < 0 || bucketId > 4095 || mapEpoch < 1
                || !validShard(ingestionShardId) || !validShard(deliveryShardId)) {
            throw new IllegalArgumentException("Invalid event locator");
        }
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(bytes);
            output.writeByte(VERSION);
            output.writeShort(bucketId);
            output.writeLong(mapEpoch);
            output.writeInt(Math.toIntExact(day.toEpochDay()));
            output.writeLong(eventId.getMostSignificantBits());
            output.writeLong(eventId.getLeastSignificantBits());
            output.writeUTF(ingestionShardId);
            output.writeUTF(deliveryShardId);
            return "evt_" + Base64.getUrlEncoder().withoutPadding().encodeToString(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    public static Decoded decode(String locator) {
        if (locator == null || !locator.startsWith("evt_") || locator.length() > 300) {
            throw new IllegalArgumentException("Invalid event locator");
        }
        try {
            byte[] bytes = Base64.getUrlDecoder().decode(locator.substring(4));
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes));
            int version = input.readUnsignedByte();
            int bucketId = input.readUnsignedShort();
            long epoch = input.readLong();
            LocalDate day = LocalDate.ofEpochDay(input.readInt());
            UUID eventId = new UUID(input.readLong(), input.readLong());
            String ingestionShard = input.readUTF();
            String deliveryShard = input.readUTF();
            if (version != VERSION || bucketId > 4095 || epoch < 1 || !validShard(ingestionShard)
                    || !validShard(deliveryShard) || input.available() != 0) {
                throw new IllegalArgumentException("Invalid event locator");
            }
            return new Decoded(version, eventId, day, bucketId, epoch, ingestionShard, deliveryShard);
        } catch (IOException | IllegalArgumentException malformed) {
            throw new IllegalArgumentException("Invalid event locator", malformed);
        }
    }

    private static boolean validShard(String value) {
        return value != null && value.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,79}");
    }

    public record Decoded(int version, UUID eventId, LocalDate acceptedDay, int bucketId,
            long mapEpoch, String ingestionShardId, String deliveryShardId) {
    }
}

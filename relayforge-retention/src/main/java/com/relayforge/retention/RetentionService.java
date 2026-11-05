package com.relayforge.retention;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import javax.sql.DataSource;

/** Coordinates owner-local transactions without deleting active event work. */
public final class RetentionService {
    private static final DateTimeFormatter DAY_NAME = DateTimeFormatter.BASIC_ISO_DATE;
    private static final long INGESTION_MAINTENANCE_LOCK = 8710440101L;
    private static final long DELIVERY_MAINTENANCE_LOCK = 8710440102L;
    private static final List<String> DELIVERY_TABLES = List.of(
            "routing_progress", "routing_targets", "deliveries", "delivery_attempts",
            "authentication_attempts", "replay_requests", "delivery_audit_records");
    private static final List<String> DELIVERY_DROP_ORDER = List.of(
            "delivery_audit_records", "replay_requests", "authentication_attempts",
            "delivery_attempts", "routing_targets", "deliveries", "routing_progress");
    private final DataSource ingestion;
    private final DataSource delivery;
    private final Clock clock;

    public RetentionService(DataSource ingestion, DataSource delivery, Clock clock) {
        this.ingestion = Objects.requireNonNull(ingestion);
        this.delivery = Objects.requireNonNull(delivery);
        this.clock = Objects.requireNonNull(clock);
    }

    public Report runOnce() {
        Instant now = clock.instant();
        LocalDate today = now.atZone(ZoneOffset.UTC).toLocalDate();
        precreate(ingestion, INGESTION_MAINTENANCE_LOCK, List.of("events"), today);
        precreate(delivery, DELIVERY_MAINTENANCE_LOCK, DELIVERY_TABLES, today);
        LocalDate fullyExpiredThrough = today.minusDays(91);
        Set<LocalDate> days = new TreeSet<>(days(ingestion, "events", "ingestion_outbox",
                fullyExpiredThrough));
        days.addAll(days(delivery, "routing_progress", "delivery_outbox",
                fullyExpiredThrough));
        int released = 0;
        int blocked = 0;
        LocalDate oldestBlockedDay = null;
        for (LocalDate day : days) {
            if (day.plusDays(91).atStartOfDay(ZoneOffset.UTC).toInstant().isAfter(now)) {
                continue;
            }
            if (releaseDelivery(day) && releaseIngestion(day, now)) {
                released++;
            } else {
                blocked++;
                if (oldestBlockedDay == null) {
                    oldestBlockedDay = day;
                }
            }
        }
        return new Report(released, blocked, oldestBlockedDay);
    }

    private void precreate(DataSource source, long lockKey, List<String> tables, LocalDate today) {
        transaction(source, connection -> {
            if (!lock(connection, lockKey)) {
                throw new SQLException("Retention partition preparation is already running");
            }
            for (int ahead = 0; ahead <= 14; ahead++) {
                LocalDate day = today.plusDays(ahead);
                for (String table : tables) {
                    if (!partitionExists(connection, table, day)) {
                        execute(connection, "CREATE TABLE " + child(table, day)
                                + " PARTITION OF " + table + " FOR VALUES FROM ('" + day
                                + "') TO ('" + day.plusDays(1) + "')");
                    }
                }
            }
            return true;
        });
    }

    private Set<LocalDate> days(DataSource source, String parent, String outbox,
            LocalDate fullyExpiredThrough) {
        Set<LocalDate> found = new TreeSet<>();
        try (Connection connection = source.getConnection();
                PreparedStatement partitions = connection.prepareStatement("""
                        SELECT child.relname FROM pg_inherits inheritance
                        JOIN pg_class child ON child.oid = inheritance.inhrelid
                        JOIN pg_class parent ON parent.oid = inheritance.inhparent
                        JOIN pg_namespace namespace ON namespace.oid = parent.relnamespace
                        WHERE namespace.nspname = 'public' AND parent.relname = ?
                        """)) {
            partitions.setString(1, parent);
            try (ResultSet rows = partitions.executeQuery()) {
                while (rows.next()) {
                    String name = rows.getString(1);
                    if (name.matches(parent + "_[0-9]{8}")) {
                        LocalDate day = LocalDate.parse(name.substring(parent.length() + 1), DAY_NAME);
                        if (!day.isAfter(fullyExpiredThrough)) {
                            found.add(day);
                        }
                    }
                }
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT DISTINCT accepted_day FROM " + outbox + " WHERE accepted_day <= ?")) {
                statement.setObject(1, fullyExpiredThrough);
                try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    found.add(rows.getObject(1, LocalDate.class));
                }
                }
            }
            return found;
        } catch (SQLException unavailable) {
            throw new IllegalStateException("Cannot inspect retention cohorts", unavailable);
        }
    }

    private boolean releaseDelivery(LocalDate day) {
        if (!ingestionReadyForDeliveryCleanup(day)) {
            return false;
        }
        return transaction(delivery, connection -> {
            if (!lock(connection, DELIVERY_MAINTENANCE_LOCK)) {
                return false;
            }
            execute(connection, "LOCK TABLE delivery_work, delivery_outbox, routing_progress, "
                    + "deliveries IN SHARE ROW EXCLUSIVE MODE");
            if (count(connection, "SELECT count(*) FROM delivery_work WHERE accepted_day = ?", day) > 0
                    || count(connection, """
                            SELECT count(*) FROM delivery_outbox WHERE accepted_day = ?
                            AND (status <> 'PUBLISHED' OR published_at IS NULL)
                            """, day) > 0
                    || count(connection, """
                            SELECT count(*) FROM deliveries WHERE accepted_day = ?
                            AND status NOT IN ('SUCCEEDED', 'DEAD_LETTERED', 'CANCELLED')
                            """, day) > 0
                    || count(connection, """
                            SELECT count(*) FROM routing_progress WHERE accepted_day = ?
                            AND status NOT IN ('ROUTED', 'NO_SUBSCRIBERS')
                            """, day) > 0) {
                return false;
            }
            update(connection, "DELETE FROM delivery_outbox WHERE accepted_day = ? "
                    + "AND status = 'PUBLISHED' AND published_at IS NOT NULL", day);
            for (String table : DELIVERY_DROP_ORDER) {
                dropPartition(connection, table, day);
            }
            return true;
        });
    }

    private boolean ingestionReadyForDeliveryCleanup(LocalDate day) {
        try (Connection ingestionConnection = ingestion.getConnection();
                Connection deliveryConnection = delivery.getConnection()) {
            long unpublished = count(ingestionConnection, """
                    SELECT count(*) FROM ingestion_outbox WHERE accepted_day = ?
                    AND (status <> 'PUBLISHED' OR published_at IS NULL)
                    """, day);
            if (unpublished != 0 || !partitionExists(deliveryConnection,
                    "routing_progress", day)) {
                return unpublished == 0;
            }
            long events = count(ingestionConnection,
                    "SELECT count(*) FROM events WHERE accepted_day = ?", day);
            long routed = count(deliveryConnection,
                    "SELECT count(*) FROM routing_progress WHERE accepted_day = ?", day);
            return unpublished == 0 && events == routed;
        } catch (SQLException unavailable) {
            throw new IllegalStateException("Cannot check routing completion", unavailable);
        }
    }

    private boolean releaseIngestion(LocalDate day, Instant now) {
        return transaction(ingestion, connection -> {
            if (!lock(connection, INGESTION_MAINTENANCE_LOCK)) {
                return false;
            }
            execute(connection, "LOCK TABLE events, ingestion_outbox, idempotency_registry "
                    + "IN SHARE ROW EXCLUSIVE MODE");
            if (count(connection, """
                    SELECT count(*) FROM ingestion_outbox WHERE accepted_day = ?
                    AND (status <> 'PUBLISHED' OR published_at IS NULL)
                    """, day) > 0) {
                return false;
            }
            update(connection, "DELETE FROM ingestion_outbox WHERE accepted_day = ? "
                    + "AND status = 'PUBLISHED' AND published_at IS NOT NULL", day);
            update(connection, "DELETE FROM idempotency_registry WHERE accepted_day = ? "
                    + "AND expires_at <= ?", day, java.sql.Timestamp.from(now));
            if (count(connection, "SELECT count(*) FROM idempotency_registry "
                    + "WHERE accepted_day = ?", day) > 0) {
                return false;
            }
            dropPartition(connection, "events", day);
            return true;
        });
    }

    private void dropPartition(Connection connection, String parent, LocalDate day) throws SQLException {
        if (partitionExists(connection, parent, day)) {
            String child = child(parent, day);
            execute(connection, "ALTER TABLE " + parent + " DETACH PARTITION " + child);
            execute(connection, "DROP TABLE " + child);
        }
    }

    private boolean partitionExists(Connection connection, String parent, LocalDate day)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT to_regclass(?) IS NOT NULL")) {
            statement.setString(1, "public." + child(parent, day));
            try (ResultSet rows = statement.executeQuery()) {
                rows.next();
                return rows.getBoolean(1);
            }
        }
    }

    private String child(String parent, LocalDate day) {
        return parent + "_" + DAY_NAME.format(day);
    }

    private boolean lock(Connection connection, long key) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT pg_try_advisory_xact_lock(?)")) {
            statement.setLong(1, key);
            try (ResultSet rows = statement.executeQuery()) {
                rows.next();
                return rows.getBoolean(1);
            }
        }
    }

    private long count(Connection connection, String sql, LocalDate day) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, day);
            try (ResultSet rows = statement.executeQuery()) {
                rows.next();
                return rows.getLong(1);
            }
        }
    }

    private void update(Connection connection, String sql, LocalDate day, Object... rest)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, day);
            for (int index = 0; index < rest.length; index++) {
                statement.setObject(index + 2, rest[index]);
            }
            statement.executeUpdate();
        }
    }

    private void execute(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private boolean transaction(DataSource source, SqlTask task) {
        try (Connection connection = source.getConnection()) {
            connection.setAutoCommit(false);
            try {
                execute(connection, "SET LOCAL lock_timeout = '1s'");
                execute(connection, "SET LOCAL statement_timeout = '15s'");
                boolean done = task.run(connection);
                connection.commit();
                return done;
            } catch (SQLException failed) {
                connection.rollback();
                throw failed;
            }
        } catch (SQLException unavailable) {
            throw new IllegalStateException("Retention maintenance failed", unavailable);
        }
    }

    @FunctionalInterface
    private interface SqlTask {
        boolean run(Connection connection) throws SQLException;
    }

    public record Report(int releasedDays, int blockedDays, LocalDate oldestBlockedDay) {
    }
}

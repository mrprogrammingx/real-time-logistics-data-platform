package com.flowfleet.flink.sink;

import com.flowfleet.events.avro.DeliveryAlert;
import com.flowfleet.events.avro.DriverSnapshot;
import com.flowfleet.events.avro.DriverSpeedWindow;
import com.flowfleet.events.avro.GeofenceEvent;
import java.sql.Timestamp;
import java.sql.Types;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Batched, retrying JDBC sinks with idempotent write SQL.
 *
 * <ul>
 *   <li><b>batching</b> — {@code JdbcExecutionOptions}: flush at 500 rows or 1 s;</li>
 *   <li><b>connection pool</b> — one connection per sink subtask, reused for the life of
 *       the operator;</li>
 *   <li><b>retries</b> — 3 attempts on a failed batch;</li>
 *   <li><b>idempotency</b> — TimescaleDB: {@code ON CONFLICT (pk incl. event time) DO NOTHING};
 *       ClickHouse: {@code ReplacingMergeTree(event_id)} collapses re-inserts at merge time.
 *       So a batch retry, or a full Flink replay from a checkpoint, converges to one row.</li>
 * </ul>
 */
public final class JdbcSinks {

    private JdbcSinks() {}

    private static final JdbcExecutionOptions EXEC = JdbcExecutionOptions.builder()
            .withBatchSize(500)
            .withBatchIntervalMs(1_000)
            .withMaxRetries(3)
            .build();

    private static JdbcConnectionOptions conn(String url, String driver, String user, String pw) {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(url).withDriverName(driver).withUsername(user).withPassword(pw).build();
    }

    // ---- TimescaleDB ---------------------------------------------------------

    public static SinkFunction<DriverSnapshot> driverState(SinkOptions o) {
        String sql = """
            INSERT INTO driver_state (driver_id, event_time, latitude, longitude,
              reported_speed_kph, derived_speed_kph, heading_degrees, vehicle_type,
              trip_distance_meters, sample_count, processing_lag_ms)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT (driver_id, event_time) DO NOTHING
            """;
        JdbcStatementBuilder<DriverSnapshot> bind = (ps, s) -> {
            ps.setLong(1, s.getDriverId());
            ps.setTimestamp(2, Timestamp.from(s.getEventTime()));
            ps.setDouble(3, s.getLatitude());
            ps.setDouble(4, s.getLongitude());
            setNullableDouble(ps, 5, s.getReportedSpeedKph());
            setNullableDouble(ps, 6, s.getDerivedSpeedKph());
            if (s.getHeadingDegrees() == null) {
                ps.setNull(7, Types.INTEGER);
            } else {
                ps.setInt(7, s.getHeadingDegrees());
            }
            ps.setString(8, s.getVehicleType());
            ps.setDouble(9, s.getTripDistanceMeters());
            ps.setLong(10, s.getSampleCount());
            ps.setLong(11, s.getProcessingLagMs());
        };
        return JdbcSink.sink(sql, bind, EXEC,
                conn(o.timescaleUrl, "org.postgresql.Driver", o.timescaleUser, o.timescalePassword));
    }

    public static SinkFunction<DriverSpeedWindow> driverSpeedWindows(SinkOptions o) {
        String sql = """
            INSERT INTO driver_speed_windows (driver_id, window_start, window_end,
              sample_count, avg_speed_kph, max_speed_kph, distance_meters)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT (driver_id, window_start) DO NOTHING
            """;
        JdbcStatementBuilder<DriverSpeedWindow> bind = (ps, w) -> {
            ps.setLong(1, w.getDriverId());
            ps.setTimestamp(2, Timestamp.from(w.getWindowStart()));
            ps.setTimestamp(3, Timestamp.from(w.getWindowEnd()));
            ps.setLong(4, w.getSampleCount());
            ps.setDouble(5, w.getAvgSpeedKph());
            ps.setDouble(6, w.getMaxSpeedKph());
            ps.setDouble(7, w.getDistanceMeters());
        };
        return JdbcSink.sink(sql, bind, EXEC,
                conn(o.timescaleUrl, "org.postgresql.Driver", o.timescaleUser, o.timescalePassword));
    }

    // ---- ClickHouse ---------------------------------------------------------

    public static SinkFunction<GeofenceEvent> geofenceEvents(SinkOptions o) {
        String sql = """
            INSERT INTO flowfleet.geofence_events
              (event_id, driver_id, geofence_id, geofence_name, geofence_type,
               transition, latitude, longitude, event_time)
            VALUES (?,?,?,?,?,?,?,?,?)
            """;
        JdbcStatementBuilder<GeofenceEvent> bind = (ps, e) -> {
            ps.setString(1, e.getEventId());
            ps.setLong(2, e.getDriverId());
            ps.setLong(3, e.getGeofenceId());
            ps.setString(4, e.getGeofenceName());
            ps.setString(5, e.getGeofenceType());
            ps.setString(6, e.getTransition().name());
            ps.setDouble(7, e.getLatitude());
            ps.setDouble(8, e.getLongitude());
            ps.setTimestamp(9, Timestamp.from(e.getEventTime()));
        };
        return JdbcSink.sink(sql, bind, EXEC,
                conn(o.clickHouseUrl, "com.clickhouse.jdbc.ClickHouseDriver",
                        o.clickHouseUser, o.clickHousePassword));
    }

    public static SinkFunction<DeliveryAlert> deliveryAlerts(SinkOptions o) {
        String sql = """
            INSERT INTO flowfleet.delivery_alerts
              (event_id, kind, order_id, driver_id, detail, event_time)
            VALUES (?,?,?,?,?,?)
            """;
        JdbcStatementBuilder<DeliveryAlert> bind = (ps, a) -> {
            ps.setString(1, a.getEventId());
            ps.setString(2, a.getKind().name());
            if (a.getOrderId() == null) {
                ps.setNull(3, Types.BIGINT);
            } else {
                ps.setLong(3, a.getOrderId());
            }
            if (a.getDriverId() == null) {
                ps.setNull(4, Types.BIGINT);
            } else {
                ps.setLong(4, a.getDriverId());
            }
            ps.setString(5, a.getDetail());
            ps.setTimestamp(6, Timestamp.from(a.getEventTime()));
        };
        return JdbcSink.sink(sql, bind, EXEC,
                conn(o.clickHouseUrl, "com.clickhouse.jdbc.ClickHouseDriver",
                        o.clickHouseUser, o.clickHousePassword));
    }

    private static void setNullableDouble(java.sql.PreparedStatement ps, int idx, Double v)
            throws java.sql.SQLException {
        if (v == null) {
            ps.setNull(idx, Types.DOUBLE);
        } else {
            ps.setDouble(idx, v);
        }
    }
}

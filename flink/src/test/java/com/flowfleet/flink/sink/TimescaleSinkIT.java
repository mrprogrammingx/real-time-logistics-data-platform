package com.flowfleet.flink.sink;

import static org.assertj.core.api.Assertions.assertThat;

import com.flowfleet.events.avro.DriverSnapshot;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Proves the TimescaleDB sink is idempotent: writing a batch, then replaying the identical
 * batch (as a checkpoint recovery would), leaves exactly one row per
 * {@code (driver_id, event_time)} — the `ON CONFLICT DO NOTHING` upsert absorbs the replay.
 */
@Testcontainers
class TimescaleSinkIT {

    @RegisterExtension
    static final MiniClusterExtension FLINK = new MiniClusterExtension();

    @Container
    static final PostgreSQLContainer<?> TS = new PostgreSQLContainer<>(
            DockerImageName.parse("timescale/timescaledb:2.17.2-pg16").asCompatibleSubstituteFor("postgres"))
            .withDatabaseName("flowfleet").withUsername("flowfleet").withPassword("flowfleet")
            .withStartupTimeout(java.time.Duration.ofMinutes(3));

    private static final long T0 = 1_800_000_000_000L;

    @BeforeAll
    static void schema() {
        Sql.runScript(TS.getJdbcUrl(), TS.getUsername(), TS.getPassword(),
                Path.of("../database/timescaledb/init.sql"));
    }

    @Test
    void replayingTheSameBatchDoesNotDuplicateRows() throws Exception {
        SinkOptions o = SinkOptions.of(TS.getJdbcUrl(), TS.getUsername(), TS.getPassword(),
                "unused", "unused", "unused");

        List<DriverSnapshot> batch = List.of(
                snapshot(1L, T0), snapshot(1L, T0 + 1_000), snapshot(2L, T0));

        writeToTimescale(batch, o);
        writeToTimescale(batch, o);   // replay

        long rows = Sql.count(TS.getJdbcUrl(), TS.getUsername(), TS.getPassword(),
                "SELECT count(*) FROM driver_state");
        long distinctKeys = Sql.count(TS.getJdbcUrl(), TS.getUsername(), TS.getPassword(),
                "SELECT count(*) FROM (SELECT DISTINCT driver_id, event_time FROM driver_state) x");

        assertThat(rows).isEqualTo(3);
        assertThat(distinctKeys).isEqualTo(3);

        long latestForDriver1 = Sql.count(TS.getJdbcUrl(), TS.getUsername(), TS.getPassword(),
                "SELECT sample_count FROM driver_current_state WHERE driver_id = 1");
        assertThat(latestForDriver1).isEqualTo(1);
    }

    private static void writeToTimescale(List<DriverSnapshot> batch, SinkOptions o) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.fromData(batch).addSink(JdbcSinks.driverState(o)).name("timescale");
        env.execute("timescale-sink-it");
    }

    private static DriverSnapshot snapshot(long driverId, long eventTimeMs) {
        return DriverSnapshot.newBuilder()
                .setDriverId(driverId)
                .setLatitude(40.1776).setLongitude(44.5126)
                .setEventTime(Instant.ofEpochMilli(eventTimeMs))
                .setTripDistanceMeters(12.3)
                .setSampleCount(1)
                .setProcessingLagMs(5)
                .build();
    }
}

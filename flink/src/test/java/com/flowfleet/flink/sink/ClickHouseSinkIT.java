package com.flowfleet.flink.sink;

import static org.assertj.core.api.Assertions.assertThat;

import com.flowfleet.events.avro.GeofenceEvent;
import com.flowfleet.events.avro.GeofenceTransition;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Proves the ClickHouse sink deduplicates on {@code event_id}: inserting the same event
 * twice (a replay) plus one distinct event yields two rows once
 * {@code ReplacingMergeTree} is forced to collapse with {@code FINAL}.
 */
@Testcontainers
class ClickHouseSinkIT {

    @RegisterExtension
    static final MiniClusterExtension FLINK = new MiniClusterExtension();

    @Container
    static final ClickHouseContainer CH = new ClickHouseContainer(
            DockerImageName.parse("clickhouse/clickhouse-server:24.8"))
            .withDatabaseName("flowfleet")
            .withUsername("flowfleet")
            .withPassword("flowfleet")
            .withStartupTimeout(java.time.Duration.ofMinutes(3));

    private static final long T0 = 1_800_000_000_000L;

    @BeforeAll
    static void schema() {
        Sql.runScript(CH.getJdbcUrl(), CH.getUsername(), CH.getPassword(),
                Path.of("../database/clickhouse/init.sql"));
    }

    @Test
    void duplicateEventIdsCollapse() throws Exception {
        SinkOptions o = SinkOptions.of("unused", "unused", "unused",
                CH.getJdbcUrl(), CH.getUsername(), CH.getPassword());

        List<GeofenceEvent> batch = List.of(
                geofence("abc123", 1L, GeofenceTransition.ENTER),
                geofence("abc123", 1L, GeofenceTransition.ENTER),   // replay — same event_id
                geofence("def456", 2L, GeofenceTransition.EXIT));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.fromData(batch).addSink(JdbcSinks.geofenceEvents(o)).name("clickhouse");
        env.execute("clickhouse-sink-it");

        long raw = Sql.count(CH.getJdbcUrl(), CH.getUsername(), CH.getPassword(),
                "SELECT count() FROM flowfleet.geofence_events");
        long deduped = Sql.count(CH.getJdbcUrl(), CH.getUsername(), CH.getPassword(),
                "SELECT count() FROM flowfleet.geofence_events FINAL");
        long distinctIds = Sql.count(CH.getJdbcUrl(), CH.getUsername(), CH.getPassword(),
                "SELECT uniqExact(event_id) FROM flowfleet.geofence_events");

        assertThat(raw).isGreaterThanOrEqualTo(2);   // 2 or 3 depending on background merges
        assertThat(deduped).isEqualTo(2);
        assertThat(distinctIds).isEqualTo(2);
    }

    private static GeofenceEvent geofence(String eventId, long driverId, GeofenceTransition t) {
        return GeofenceEvent.newBuilder()
                .setEventId(eventId)
                .setDriverId(driverId)
                .setGeofenceId(1L)
                .setGeofenceName("Republic Square")
                .setGeofenceType("RESTAURANT")
                .setTransition(t)
                .setLatitude(40.1776).setLongitude(44.5126)
                .setEventTime(Instant.ofEpochMilli(T0))
                .build();
    }
}

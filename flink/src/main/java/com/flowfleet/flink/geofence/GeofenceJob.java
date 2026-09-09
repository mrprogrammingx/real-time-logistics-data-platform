package com.flowfleet.flink.geofence;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowfleet.events.Topics;
import com.flowfleet.events.avro.DriverLocation;
import com.flowfleet.events.avro.GeofenceEvent;
import com.flowfleet.flink.GpsGuard;
import com.flowfleet.flink.JobConfig;
import com.flowfleet.flink.KafkaIO;
import com.flowfleet.flink.Watermarks;
import com.flowfleet.flink.geo.Geofence;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * {@code flowfleet.driver.locations} + broadcast geofences → ENTER/EXIT →
 * {@code flowfleet.driver.geofence-events}.
 *
 * <p>Phase 4 loads the geofence definitions from a bundled resource (they change rarely and
 * match the seeded rows). The production wiring is a {@code flowfleet.geofences.cdc}
 * source — same {@link GeofenceFunction}, different broadcast input.
 */
public final class GeofenceJob {

    public static final String NAME = "geofence";

    private GeofenceJob() {}

    public static void main(String[] args) throws Exception {
        JobConfig cfg = JobConfig.from(args);
        StreamExecutionEnvironment env = JobConfig.environment();
        build(env, cfg, loadGeofences());
        env.execute("flowfleet-" + NAME);
    }

    public static void build(StreamExecutionEnvironment env, JobConfig cfg, List<Geofence> fences) {
        BroadcastStream<Geofence> geofences = env.fromData(fences).broadcast(GeofenceFunction.FENCES);

        DataStream<DriverLocation> locations = env
                .fromSource(
                        KafkaIO.avroSource(cfg, Topics.DRIVER_LOCATIONS, cfg.groupId(NAME), DriverLocation.class),
                        Watermarks.forDriverLocation(),
                        "driver.locations")
                .process(new GpsGuard())
                .name("gps-guard");

        locations.keyBy(DriverLocation::getDriverId)
                .connect(geofences)
                .process(new GeofenceFunction())
                .name("geofence")
                .sinkTo(KafkaIO.avroSink(cfg, Topics.DRIVER_GEOFENCE_EVENTS, GeofenceEvent.class,
                        e -> String.valueOf(e.getDriverId())))
                .name("driver.geofence-events");
    }

    public static List<Geofence> loadGeofences() throws IOException {
        try (InputStream in = GeofenceJob.class.getResourceAsStream("/geofences.json")) {
            return new ObjectMapper().readValue(in, new TypeReference<List<Geofence>>() {});
        }
    }
}

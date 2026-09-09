package com.flowfleet.flink.driverstate;

import com.flowfleet.events.Topics;
import com.flowfleet.events.avro.DriverLocation;
import com.flowfleet.events.avro.DriverSnapshot;
import com.flowfleet.flink.JobConfig;
import com.flowfleet.flink.KafkaIO;
import com.flowfleet.flink.Tags;
import com.flowfleet.flink.Watermarks;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * {@code flowfleet.driver.locations} → keyed driver state → {@code flowfleet.driver.state}.
 *
 * <pre>
 *   Kafka(DriverLocation) --watermarks--> GpsGuard --valid--> keyBy(driverId) --> DriverStateFunction --> Kafka(DriverSnapshot)
 *                                                \--invalid--> flowfleet.driver.locations.dlq
 * </pre>
 *
 * Run:  {@code flink run -c com.flowfleet.flink.driverstate.DriverStateJob flowfleet-flink-jobs.jar}
 */
public final class DriverStateJob {

    public static final String NAME = "driver-state";

    private DriverStateJob() {}

    public static void main(String[] args) throws Exception {
        JobConfig cfg = JobConfig.from(args);
        StreamExecutionEnvironment env = JobConfig.environment();
        build(env, cfg);
        env.execute("flowfleet-" + NAME);
    }

    /** Wires the topology onto {@code env} (extracted so tests can run it on a MiniCluster). */
    public static void build(StreamExecutionEnvironment env, JobConfig cfg) {
        SingleOutputStreamOperator<DriverLocation> valid = env
                .fromSource(
                        KafkaIO.avroSource(cfg, Topics.DRIVER_LOCATIONS, cfg.groupId(NAME), DriverLocation.class),
                        Watermarks.forDriverLocation(),
                        "driver.locations")
                .process(new com.flowfleet.flink.GpsGuard())
                .name("gps-guard");

        valid.getSideOutput(Tags.DLQ_LOCATIONS)
                .sinkTo(KafkaIO.stringSink(cfg, Topics.DRIVER_LOCATIONS_DLQ))
                .name("dlq");

        valid.keyBy(DriverLocation::getDriverId)
                .process(new DriverStateFunction())
                .name("driver-state")
                .sinkTo(KafkaIO.avroSink(cfg, Topics.DRIVER_STATE, DriverSnapshot.class,
                        s -> String.valueOf(s.getDriverId())))
                .name("driver.state");
    }
}

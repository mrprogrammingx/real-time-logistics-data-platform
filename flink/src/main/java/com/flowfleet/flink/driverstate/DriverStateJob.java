package com.flowfleet.flink.driverstate;

import com.flowfleet.events.Topics;
import com.flowfleet.events.avro.DriverLocation;
import com.flowfleet.events.avro.DriverSnapshot;
import com.flowfleet.flink.JobConfig;
import com.flowfleet.flink.KafkaIO;
import com.flowfleet.flink.ingest.LocationSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * {@code flowfleet.driver.locations} → keyed driver state → {@code flowfleet.driver.state}.
 *
 * <pre>
 *   Kafka --LocationSource.ingest--> keyBy(driverId) --> DriverStateFunction --> Kafka(DriverSnapshot)
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
        SingleOutputStreamOperator<DriverLocation> locations = LocationSource.ingest(env, cfg, NAME);

        locations.keyBy(DriverLocation::getDriverId)
                .process(new DriverStateFunction())
                .name("driver-state")
                .sinkTo(KafkaIO.avroSink(cfg, Topics.DRIVER_STATE, DriverSnapshot.class,
                        s -> String.valueOf(s.getDriverId())))
                .name("driver.state");
    }
}

package com.flowfleet.flink.anomaly;

import com.flowfleet.events.Topics;
import com.flowfleet.events.avro.DeliveryAlert;
import com.flowfleet.events.avro.DriverLocation;
import com.flowfleet.flink.JobConfig;
import com.flowfleet.flink.KafkaIO;
import com.flowfleet.flink.ingest.LocationSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * {@code flowfleet.driver.locations} → per-driver anomaly detection (timers + rules) →
 * {@code flowfleet.delivery.alerts}.
 */
public final class AnomalyJob {

    public static final String NAME = "anomaly";

    private AnomalyJob() {}

    public static void main(String[] args) throws Exception {
        JobConfig cfg = JobConfig.from(args);
        StreamExecutionEnvironment env = JobConfig.environment();
        build(env, cfg);
        env.execute("flowfleet-" + NAME);
    }

    public static void build(StreamExecutionEnvironment env, JobConfig cfg) {
        LocationSource.ingest(env, cfg, NAME)
                .keyBy(DriverLocation::getDriverId)
                .process(new AnomalyFunction())
                .name("anomaly")
                .sinkTo(KafkaIO.avroSink(cfg, Topics.DELIVERY_ALERTS, DeliveryAlert.class,
                        a -> String.valueOf(a.getDriverId())))
                .name("delivery.alerts");
    }
}

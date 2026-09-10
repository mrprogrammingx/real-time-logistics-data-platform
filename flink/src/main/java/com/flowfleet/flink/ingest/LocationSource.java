package com.flowfleet.flink.ingest;

import com.flowfleet.events.Topics;
import com.flowfleet.events.avro.DriverLocation;
import com.flowfleet.flink.JobConfig;
import com.flowfleet.flink.KafkaIO;
import com.flowfleet.flink.Tags;
import com.flowfleet.flink.Watermarks;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * The shared front of every processing job:
 *
 * <pre>
 *   Kafka(bytes) --resilient decode--> LocationIngest --valid--> watermarks --> (caller)
 *                                              \--decode fail / invalid--> flowfleet.driver.locations.dlq
 * </pre>
 *
 * A poison message never fails the job — it lands on the DLQ with its Kafka offset and the
 * decode error. Watermarks are assigned only on the validated stream.
 */
public final class LocationSource {

    private LocationSource() {}

    public static SingleOutputStreamOperator<DriverLocation> ingest(
            StreamExecutionEnvironment env, JobConfig cfg, String jobName) {

        SingleOutputStreamOperator<DriverLocation> ingested = env
                .fromSource(
                        KafkaIO.resilientLocationSource(cfg, cfg.groupId(jobName)),
                        WatermarkStrategy.noWatermarks(),
                        "driver.locations")
                .process(new LocationIngest())
                .name("ingest");

        ingested.getSideOutput(Tags.DLQ_LOCATIONS)
                .sinkTo(KafkaIO.stringSink(cfg, Topics.DRIVER_LOCATIONS_DLQ))
                .name("dlq");

        return ingested
                .assignTimestampsAndWatermarks(Watermarks.forDriverLocation())
                .name("watermarks");
    }
}

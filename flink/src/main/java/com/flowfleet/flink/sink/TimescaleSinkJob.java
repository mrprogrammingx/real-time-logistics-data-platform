package com.flowfleet.flink.sink;

import com.flowfleet.events.Topics;
import com.flowfleet.events.avro.DriverSnapshot;
import com.flowfleet.events.avro.DriverSpeedWindow;
import com.flowfleet.flink.JobConfig;
import com.flowfleet.flink.KafkaIO;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * {@code flowfleet.driver.state} + {@code flowfleet.driver.speed-windows} → TimescaleDB
 * hypertables (`driver_state`, `driver_speed_windows`). One job, two pipelines.
 */
public final class TimescaleSinkJob {

    public static final String NAME = "timescale-sink";

    private TimescaleSinkJob() {}

    public static void main(String[] args) throws Exception {
        JobConfig cfg = JobConfig.from(args);
        SinkOptions sinks = SinkOptions.from(args);
        StreamExecutionEnvironment env = JobConfig.environment();
        build(env, cfg, sinks);
        env.execute("flowfleet-" + NAME);
    }

    public static void build(StreamExecutionEnvironment env, JobConfig cfg, SinkOptions sinks) {
        env.fromSource(
                        KafkaIO.avroSource(cfg, Topics.DRIVER_STATE, cfg.groupId(NAME), DriverSnapshot.class),
                        WatermarkStrategy.noWatermarks(), "driver.state")
                .addSink(JdbcSinks.driverState(sinks))
                .name("timescale.driver_state");

        env.fromSource(
                        KafkaIO.avroSource(cfg, Topics.DRIVER_SPEED_WINDOWS, cfg.groupId(NAME), DriverSpeedWindow.class),
                        WatermarkStrategy.noWatermarks(), "driver.speed-windows")
                .addSink(JdbcSinks.driverSpeedWindows(sinks))
                .name("timescale.driver_speed_windows");
    }
}

package com.flowfleet.flink.sink;

import com.flowfleet.events.Topics;
import com.flowfleet.events.avro.DeliveryAlert;
import com.flowfleet.events.avro.GeofenceEvent;
import com.flowfleet.flink.JobConfig;
import com.flowfleet.flink.KafkaIO;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * {@code flowfleet.driver.geofence-events} + {@code flowfleet.delivery.alerts} → ClickHouse
 * {@code ReplacingMergeTree} tables. Re-inserts collapse on {@code event_id} at merge time.
 */
public final class ClickHouseSinkJob {

    public static final String NAME = "clickhouse-sink";

    private ClickHouseSinkJob() {}

    public static void main(String[] args) throws Exception {
        JobConfig cfg = JobConfig.from(args);
        SinkOptions sinks = SinkOptions.from(args);
        StreamExecutionEnvironment env = JobConfig.environment();
        build(env, cfg, sinks);
        env.execute("flowfleet-" + NAME);
    }

    public static void build(StreamExecutionEnvironment env, JobConfig cfg, SinkOptions sinks) {
        env.fromSource(
                        KafkaIO.avroSource(cfg, Topics.DRIVER_GEOFENCE_EVENTS, cfg.groupId(NAME), GeofenceEvent.class),
                        WatermarkStrategy.noWatermarks(), "driver.geofence-events")
                .addSink(JdbcSinks.geofenceEvents(sinks))
                .name("clickhouse.geofence_events");

        env.fromSource(
                        KafkaIO.avroSource(cfg, Topics.DELIVERY_ALERTS, cfg.groupId(NAME), DeliveryAlert.class),
                        WatermarkStrategy.noWatermarks(), "delivery.alerts")
                .addSink(JdbcSinks.deliveryAlerts(sinks))
                .name("clickhouse.delivery_alerts");
    }
}

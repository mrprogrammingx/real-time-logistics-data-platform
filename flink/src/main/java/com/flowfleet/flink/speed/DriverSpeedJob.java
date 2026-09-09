package com.flowfleet.flink.speed;

import com.flowfleet.events.Topics;
import com.flowfleet.events.avro.DriverLocation;
import com.flowfleet.events.avro.DriverSpeedWindow;
import com.flowfleet.flink.JobConfig;
import com.flowfleet.flink.KafkaIO;
import com.flowfleet.flink.Tags;
import com.flowfleet.flink.Watermarks;
import com.flowfleet.flink.ingest.LocationSource;
import java.time.Duration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

/**
 * {@code flowfleet.driver.locations} → 1-minute tumbling event-time windows per driver →
 * {@code flowfleet.driver.speed-windows}.
 *
 * <p>Event time + watermarks ({@link Watermarks}); samples later than the window's
 * watermark are routed to {@link Tags#LATE_LOCATIONS}, not dropped.
 */
public final class DriverSpeedJob {

    public static final String NAME = "driver-speed";
    public static final Duration WINDOW = Duration.ofMinutes(1);

    private DriverSpeedJob() {}

    public static void main(String[] args) throws Exception {
        JobConfig cfg = JobConfig.from(args);
        StreamExecutionEnvironment env = JobConfig.environment();
        build(env, cfg);
        env.execute("flowfleet-" + NAME);
    }

    public static void build(StreamExecutionEnvironment env, JobConfig cfg) {
        SingleOutputStreamOperator<DriverSpeedWindow> windows = LocationSource.ingest(env, cfg, NAME)
                .keyBy(DriverLocation::getDriverId)
                .window(TumblingEventTimeWindows.of(WINDOW))
                .sideOutputLateData(Tags.LATE_LOCATIONS)
                .process(new SpeedWindowFunction())
                .name("speed-window");

        windows.sinkTo(KafkaIO.avroSink(cfg, Topics.DRIVER_SPEED_WINDOWS, DriverSpeedWindow.class,
                        w -> String.valueOf(w.getDriverId())))
                .name("driver.speed-windows");

        DataStream<DriverLocation> late = windows.getSideOutput(Tags.LATE_LOCATIONS);
        late.map(l -> "{\"late\":true,\"driverId\":" + l.getDriverId()
                        + ",\"eventTime\":\"" + l.getEventTime() + "\"}")
                .sinkTo(KafkaIO.stringSink(cfg, Topics.DRIVER_LOCATIONS_DLQ))
                .name("late-data");
    }
}

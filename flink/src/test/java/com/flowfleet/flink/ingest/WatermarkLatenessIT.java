package com.flowfleet.flink.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import com.flowfleet.events.avro.DriverLocation;
import com.flowfleet.events.avro.DriverSpeedWindow;
import com.flowfleet.flink.Locations;
import com.flowfleet.flink.Tags;
import com.flowfleet.flink.speed.SpeedWindowFunction;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * A late event — one that arrives after the window's watermark has passed — is routed to
 * {@link Tags#LATE_LOCATIONS}, never dropped and never counted in the window it missed.
 *
 * <p>Uses a source that emits watermarks under test control so "late" is deterministic.
 */
class WatermarkLatenessIT {

    @RegisterExtension
    static final MiniClusterExtension FLINK = new MiniClusterExtension();

    static final ConcurrentLinkedQueue<DriverLocation> LATE = new ConcurrentLinkedQueue<>();

    private static final long T0 = 1_800_000_000_000L; // minute boundary

    @BeforeEach
    void reset() {
        LATE.clear();
    }

    @Test
    void aSampleBehindTheWatermarkGoesToTheLateSideOutput() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // window 1 = [T0, T0+60s)
        List<Step> script = List.of(
                new Step(Locations.at(7, 40.1776, 44.5126, T0 + 5_000), -1),
                new Step(Locations.at(7, 40.1780, 44.5126, T0 + 10_000), T0 + 9_000),
                new Step(Locations.at(7, 40.1900, 44.5126, T0 + 70_000), T0 + 65_000), // window 2; watermark now past window 1
                new Step(Locations.at(7, 40.1785, 44.5126, T0 + 30_000), -1));         // LATE for window 1

        SingleOutputStreamOperator<DriverSpeedWindow> windows = env
                .addSource(new ScriptedSource(script))
                .keyBy(DriverLocation::getDriverId)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .sideOutputLateData(Tags.LATE_LOCATIONS)
                .process(new SpeedWindowFunction());

        windows.getSideOutput(Tags.LATE_LOCATIONS).addSink(new Collect());

        List<DriverSpeedWindow> results = new ArrayList<>();
        try (CloseableIterator<DriverSpeedWindow> it = windows.executeAndCollect()) {
            it.forEachRemaining(results::add);
        }

        DriverSpeedWindow window1 = results.stream()
                .filter(w -> w.getWindowStart().toEpochMilli() == T0).findFirst().orElseThrow();
        assertThat(window1.getSampleCount()).isEqualTo(2L);            // e1, e2 — not the late e4

        assertThat(LATE).singleElement()
                .satisfies(l -> assertThat(l.getEventTime().toEpochMilli()).isEqualTo(T0 + 30_000));
    }

    private record Step(DriverLocation event, long watermarkAfter) implements java.io.Serializable {}

    private static final class ScriptedSource implements SourceFunction<DriverLocation> {
        private final List<Step> script;
        ScriptedSource(List<Step> script) {
            this.script = script;
        }

        @Override
        public void run(SourceContext<DriverLocation> ctx) {
            for (Step s : script) {
                ctx.collectWithTimestamp(s.event(), s.event().getEventTime().toEpochMilli());
                if (s.watermarkAfter() >= 0) {
                    ctx.emitWatermark(new org.apache.flink.streaming.api.watermark.Watermark(s.watermarkAfter()));
                }
            }
            ctx.emitWatermark(org.apache.flink.streaming.api.watermark.Watermark.MAX_WATERMARK);
        }

        @Override
        public void cancel() {}
    }

    private static final class Collect implements SinkFunction<DriverLocation> {
        @Override
        public void invoke(DriverLocation value, Context context) {
            LATE.add(value);
        }
    }
}

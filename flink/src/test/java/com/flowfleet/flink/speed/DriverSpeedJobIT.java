package com.flowfleet.flink.speed;

import static org.assertj.core.api.Assertions.assertThat;

import com.flowfleet.events.avro.DriverLocation;
import com.flowfleet.events.avro.DriverSpeedWindow;
import com.flowfleet.flink.Locations;
import com.flowfleet.flink.Watermarks;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Runs the speed-window topology on an embedded Flink MiniCluster (no Kafka): proves the
 * event-time windowing and the per-leg aggregation in {@link SpeedWindowFunction}, including
 * that out-of-order samples within the 5 s bound still land in the right window.
 */
class DriverSpeedJobIT {

    @RegisterExtension
    static final MiniClusterExtension FLINK = new MiniClusterExtension();

    private static final long T0 = 1_800_000_000_000L; // aligned to a minute boundary

    private static List<DriverSpeedWindow> run(List<DriverLocation> input) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<DriverSpeedWindow> windows = env
                .fromData(input)
                .assignTimestampsAndWatermarks(Watermarks.forDriverLocation())
                .keyBy(DriverLocation::getDriverId)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .process(new SpeedWindowFunction());

        List<DriverSpeedWindow> out = new ArrayList<>();
        try (CloseableIterator<DriverSpeedWindow> it = windows.executeAndCollect()) {
            it.forEachRemaining(out::add);
        }
        out.sort(Comparator.comparing(DriverSpeedWindow::getWindowStart));
        return out;
    }

    @Test
    void aggregatesOneWindowFromSlightlyOutOfOrderSamples() throws Exception {
        // ~28 m every 2 s along a line => ~50 km/h, all inside the first minute
        List<DriverLocation> in = List.of(
                Locations.at(7, 40.1776, 44.5126, T0),
                Locations.at(7, 40.1781, 44.5126, T0 + 4_000),   // out of order:
                Locations.at(7, 40.1779, 44.5126, T0 + 2_000),   // this one is 2 s behind the previous
                Locations.at(7, 40.1783, 44.5126, T0 + 6_000),
                Locations.at(7, 40.1786, 44.5126, T0 + 8_000));

        List<DriverSpeedWindow> windows = run(in);

        assertThat(windows).singleElement().satisfies(w -> {
            assertThat(w.getDriverId()).isEqualTo(7L);
            assertThat(w.getSampleCount()).isEqualTo(5L);
            assertThat(w.getDistanceMeters()).isBetween(90.0, 140.0);
            assertThat(w.getAvgSpeedKph()).isBetween(20.0, 90.0);
            assertThat(w.getMaxSpeedKph()).isGreaterThanOrEqualTo(w.getAvgSpeedKph());
        });
    }

    @Test
    void samplesSpanningTwoMinutesProduceTwoTumblingWindows() throws Exception {
        List<DriverLocation> in = List.of(
                Locations.at(7, 40.1776, 44.5126, T0 + 10_000),
                Locations.at(7, 40.1786, 44.5126, T0 + 40_000),
                Locations.at(7, 40.1800, 44.5126, T0 + 70_000),   // second window
                Locations.at(7, 40.1820, 44.5126, T0 + 100_000));

        List<DriverSpeedWindow> windows = run(in);

        assertThat(windows).hasSize(2);
        assertThat(windows.get(0).getWindowEnd()).isEqualTo(windows.get(1).getWindowStart());
        assertThat(windows.get(0).getSampleCount()).isEqualTo(2L);
        assertThat(windows.get(1).getSampleCount()).isEqualTo(2L);
    }
}

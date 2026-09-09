package com.flowfleet.flink.speed;

import com.flowfleet.common.domain.GeoPoint;
import com.flowfleet.events.avro.DriverLocation;
import com.flowfleet.events.avro.DriverSpeedWindow;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Per-driver movement over one event-time window: sorts the window's samples by event time,
 * then sums the leg distances and derives speed from distance/time — order-correct, unlike
 * accumulating in {@code AggregateFunction.add}. At ~1 sample/s a 1-minute window holds
 * ~60 elements, so buffering them is fine.
 */
public class SpeedWindowFunction
        extends ProcessWindowFunction<DriverLocation, DriverSpeedWindow, Long, TimeWindow> {

    @Override
    public void process(Long driverId, Context ctx, Iterable<DriverLocation> elements,
                        Collector<DriverSpeedWindow> out) {

        List<DriverLocation> sorted = new ArrayList<>();
        elements.forEach(sorted::add);
        sorted.sort(Comparator.comparingLong(e -> e.getEventTime().toEpochMilli()));

        double distanceMeters = 0.0;
        double maxLegKph = 0.0;
        long movingMillis = 0L;

        for (int i = 1; i < sorted.size(); i++) {
            DriverLocation a = sorted.get(i - 1);
            DriverLocation b = sorted.get(i);
            double leg = new GeoPoint(a.getLatitude(), a.getLongitude())
                    .distanceMetersTo(new GeoPoint(b.getLatitude(), b.getLongitude()));
            long dtMs = b.getEventTime().toEpochMilli() - a.getEventTime().toEpochMilli();
            distanceMeters += leg;
            if (dtMs > 0) {
                movingMillis += dtMs;
                double kph = leg / (dtMs / 1000.0) * 3.6;
                maxLegKph = Math.max(maxLegKph, kph);
            }
        }

        double avgKph = movingMillis > 0 ? distanceMeters / (movingMillis / 1000.0) * 3.6 : 0.0;

        out.collect(DriverSpeedWindow.newBuilder()
                .setDriverId(driverId)
                .setWindowStart(Instant.ofEpochMilli(ctx.window().getStart()))
                .setWindowEnd(Instant.ofEpochMilli(ctx.window().getEnd()))
                .setSampleCount(sorted.size())
                .setAvgSpeedKph(round(avgKph))
                .setMaxSpeedKph(round(maxLegKph))
                .setDistanceMeters(round(distanceMeters))
                .build());
    }

    private static double round(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}

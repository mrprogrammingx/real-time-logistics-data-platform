package com.flowfleet.flink.driverstate;

import com.flowfleet.common.domain.GeoPoint;
import com.flowfleet.events.avro.DriverLocation;
import com.flowfleet.events.avro.DriverSnapshot;
import java.time.Instant;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Per-driver keyed state, updated on every GPS sample.
 *
 * <p>Demonstrates: {@code keyBy(driverId)} + {@link ValueState}; deriving a value
 * (instantaneous speed) from the delta between consecutive events using event time; and an
 * event-time timer that expires state when a driver goes silent.
 */
public class DriverStateFunction
        extends KeyedProcessFunction<Long, DriverLocation, DriverSnapshot> {

    /** A driver with no sample for this long (event time) is considered offline; state is dropped. */
    public static final long GAP_MS = 120_000L;

    private transient ValueState<DriverStateAcc> accState;

    @Override
    public void open(OpenContext ctx) {
        accState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("driver-acc", DriverStateAcc.class));
    }

    @Override
    public void processElement(DriverLocation loc, Context ctx, Collector<DriverSnapshot> out)
            throws Exception {

        DriverStateAcc acc = accState.value();
        if (acc == null) {
            acc = new DriverStateAcc();
        }

        long eventTime = loc.getEventTime().toEpochMilli();
        Double derivedSpeedKph = null;
        double stepMeters = 0.0;

        if (acc.sampleCount > 0) {
            stepMeters = new GeoPoint(acc.lat, acc.lon)
                    .distanceMetersTo(new GeoPoint(loc.getLatitude(), loc.getLongitude()));
            long dtMs = eventTime - acc.lastEventTime;
            if (dtMs > 0) {
                derivedSpeedKph = stepMeters / (dtMs / 1000.0) * 3.6;
            }
        }

        acc.prevLat = acc.lat;
        acc.prevLon = acc.lon;
        acc.hasPrev = acc.sampleCount > 0;
        acc.lat = loc.getLatitude();
        acc.lon = loc.getLongitude();
        acc.reportedSpeedKph = loc.getSpeedKph();
        acc.headingDegrees = loc.getHeadingDegrees();
        acc.vehicleType = loc.getVehicleType();
        acc.lastEventTime = eventTime;
        acc.sampleCount++;
        acc.tripDistanceMeters += stepMeters;

        if (acc.gapTimer != 0L) {
            ctx.timerService().deleteEventTimeTimer(acc.gapTimer);
        }
        acc.gapTimer = eventTime + GAP_MS;
        ctx.timerService().registerEventTimeTimer(acc.gapTimer);

        accState.update(acc);

        out.collect(toSnapshot(loc.getDriverId(), acc, derivedSpeedKph,
                ctx.timerService().currentProcessingTime()));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<DriverSnapshot> out)
            throws Exception {
        DriverStateAcc acc = accState.value();
        if (acc != null && timestamp == acc.gapTimer) {
            // no sample arrived to reschedule the timer -> the driver is dark
            accState.clear();
        }
    }

    private static DriverSnapshot toSnapshot(long driverId, DriverStateAcc acc,
                                             Double derivedSpeedKph, long nowMs) {
        return DriverSnapshot.newBuilder()
                .setDriverId(driverId)
                .setLatitude(acc.lat)
                .setLongitude(acc.lon)
                .setPrevLatitude(acc.hasPrev ? acc.prevLat : null)
                .setPrevLongitude(acc.hasPrev ? acc.prevLon : null)
                .setReportedSpeedKph(acc.reportedSpeedKph)
                .setDerivedSpeedKph(derivedSpeedKph)
                .setHeadingDegrees(acc.headingDegrees)
                .setVehicleType(acc.vehicleType)
                .setTripDistanceMeters(acc.tripDistanceMeters)
                .setSampleCount(acc.sampleCount)
                .setEventTime(Instant.ofEpochMilli(acc.lastEventTime))
                .setProcessingLagMs(Math.max(0, nowMs - acc.lastEventTime))
                .build();
    }
}

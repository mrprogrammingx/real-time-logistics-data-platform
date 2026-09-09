package com.flowfleet.flink.anomaly;

import com.flowfleet.common.domain.GeoPoint;
import com.flowfleet.events.avro.AlertKind;
import com.flowfleet.events.avro.DeliveryAlert;
import com.flowfleet.events.avro.DriverLocation;
import java.time.Instant;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Per-driver anomaly detection:
 * <ul>
 *   <li><b>impossible speed</b> — consecutive samples imply &gt; {@value #MAX_KPH} km/h;</li>
 *   <li><b>GPS gap</b> — an event-time timer that fires if no sample arrives for
 *       {@value #GAP_MS} ms of event time.</li>
 * </ul>
 * Both emit a {@link DeliveryAlert} to {@code flowfleet.delivery.alerts}.
 */
public class AnomalyFunction extends KeyedProcessFunction<Long, DriverLocation, DeliveryAlert> {

    public static final double MAX_KPH = 150.0;
    public static final long GAP_MS = 120_000L;

    private transient ValueState<AnomalyAcc> accState;

    @Override
    public void open(OpenContext ctx) {
        accState = getRuntimeContext().getState(new ValueStateDescriptor<>("anomaly-acc", AnomalyAcc.class));
    }

    @Override
    public void processElement(DriverLocation loc, Context ctx, Collector<DeliveryAlert> out)
            throws Exception {
        AnomalyAcc acc = accState.value();
        if (acc == null) {
            acc = new AnomalyAcc();
        }
        long eventTime = loc.getEventTime().toEpochMilli();

        if (acc.hasPrev) {
            double meters = new GeoPoint(acc.prevLat, acc.prevLon)
                    .distanceMetersTo(new GeoPoint(loc.getLatitude(), loc.getLongitude()));
            long dtMs = eventTime - acc.lastEventTime;
            if (dtMs > 0) {
                double kph = meters / (dtMs / 1000.0) * 3.6;
                if (kph > MAX_KPH) {
                    out.collect(alert(loc.getDriverId(), AlertKind.IMPOSSIBLE_SPEED, eventTime,
                            "%.0f km/h over %.1fs between samples".formatted(kph, dtMs / 1000.0)));
                }
            }
        }

        acc.prevLat = loc.getLatitude();
        acc.prevLon = loc.getLongitude();
        acc.lastEventTime = eventTime;
        acc.hasPrev = true;

        if (acc.gapTimer != 0L) {
            ctx.timerService().deleteEventTimeTimer(acc.gapTimer);
        }
        acc.gapTimer = eventTime + GAP_MS;
        ctx.timerService().registerEventTimeTimer(acc.gapTimer);

        accState.update(acc);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<DeliveryAlert> out)
            throws Exception {
        AnomalyAcc acc = accState.value();
        if (acc != null && timestamp == acc.gapTimer) {
            out.collect(alert(ctx.getCurrentKey(), AlertKind.GPS_GAP, timestamp,
                    "no GPS sample for %ds (event time)".formatted(GAP_MS / 1000)));
            accState.clear();
        }
    }

    private static DeliveryAlert alert(long driverId, AlertKind kind, long eventTimeMs, String detail) {
        return DeliveryAlert.newBuilder()
                .setEventId(kind.name() + ":" + driverId + ":" + eventTimeMs)
                .setKind(kind)
                .setOrderId(null)
                .setDriverId(driverId)
                .setDetail(detail)
                .setEventTime(Instant.ofEpochMilli(eventTimeMs))
                .build();
    }
}

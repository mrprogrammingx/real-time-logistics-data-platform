package com.flowfleet.flink.geofence;

import com.flowfleet.events.avro.DriverLocation;
import com.flowfleet.events.avro.GeofenceEvent;
import com.flowfleet.events.avro.GeofenceTransition;
import com.flowfleet.flink.geo.Geofence;
import com.flowfleet.flink.geo.Geometries;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.prep.PreparedGeometry;

/**
 * Detects geofence ENTER / EXIT transitions per driver.
 *
 * <ul>
 *   <li><b>broadcast side</b> — geofence definitions (WKT polygons) fan out to every
 *       subtask and land in {@link #FENCES} broadcast state;</li>
 *   <li><b>keyed side</b> — each driver's GPS sample is point-in-polygon tested against
 *       every fence; {@link #insideState} remembers which fences the driver was in, so only
 *       the <em>transitions</em> are emitted.</li>
 * </ul>
 *
 * The parsed JTS geometry is cached per subtask (rebuilt from WKT), never serialized.
 */
public class GeofenceFunction
        extends KeyedBroadcastProcessFunction<Long, DriverLocation, Geofence, GeofenceEvent> {

    public static final MapStateDescriptor<Long, Geofence> FENCES = new MapStateDescriptor<>(
            "geofences", Types.LONG, TypeInformation.of(Geofence.class));

    private transient MapState<Long, Boolean> insideState;
    private transient Map<Long, PreparedGeometry> geometryCache;

    @Override
    public void open(OpenContext ctx) {
        insideState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("inside-geofences", Types.LONG, Types.BOOLEAN));
        geometryCache = new HashMap<>();
    }

    @Override
    public void processBroadcastElement(Geofence fence, Context ctx, Collector<GeofenceEvent> out)
            throws Exception {
        ctx.getBroadcastState(FENCES).put(fence.id, fence);
        geometryCache.remove(fence.id);
    }

    @Override
    public void processElement(DriverLocation loc, ReadOnlyContext ctx, Collector<GeofenceEvent> out)
            throws Exception {
        ReadOnlyBroadcastState<Long, Geofence> fences = ctx.getBroadcastState(FENCES);

        for (Map.Entry<Long, Geofence> entry : fences.immutableEntries()) {
            Geofence fence = entry.getValue();
            boolean nowInside = Geometries.contains(
                    geometry(fence), loc.getLatitude(), loc.getLongitude());
            Boolean wasBoxed = insideState.get(fence.id);
            boolean wasInside = wasBoxed != null && wasBoxed;

            if (nowInside && !wasInside) {
                out.collect(event(loc, fence, GeofenceTransition.ENTER));
                insideState.put(fence.id, true);
            } else if (!nowInside && wasInside) {
                out.collect(event(loc, fence, GeofenceTransition.EXIT));
                insideState.put(fence.id, false);
            }
        }
    }

    private PreparedGeometry geometry(Geofence fence) {
        return geometryCache.computeIfAbsent(fence.id, id -> Geometries.prepareWkt(fence.polygonWkt));
    }

    private static GeofenceEvent event(DriverLocation loc, Geofence fence, GeofenceTransition t) {
        long ts = loc.getEventTime().toEpochMilli();
        return GeofenceEvent.newBuilder()
                .setEventId(id(loc.getDriverId(), fence.id, t.name(), ts))
                .setDriverId(loc.getDriverId())
                .setGeofenceId(fence.id)
                .setGeofenceName(fence.name)
                .setGeofenceType(fence.type)
                .setTransition(t)
                .setLatitude(loc.getLatitude())
                .setLongitude(loc.getLongitude())
                .setEventTime(Instant.ofEpochMilli(ts))
                .build();
    }

    /** Deterministic idempotency key: same input → same id after a replay. */
    static String id(long driverId, long geofenceId, String transition, long eventTimeMs) {
        String raw = driverId + "|" + geofenceId + "|" + transition + "|" + eventTimeMs;
        try {
            byte[] h = MessageDigest.getInstance("SHA-256").digest(raw.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(64);
            for (int i = 0; i < 12; i++) {
                sb.append(String.format("%02x", h[i]));
            }
            return sb.toString();
        } catch (Exception e) {
            return raw;
        }
    }
}

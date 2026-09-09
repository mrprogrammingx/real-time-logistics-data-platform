package com.flowfleet.flink.ingest;

import com.flowfleet.common.domain.GeoPoint;
import com.flowfleet.events.avro.DriverLocation;
import com.flowfleet.flink.Tags;
import org.apache.flink.metrics.Counter;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * The single ingestion gate for the location stream. Replaces the old {@code GpsGuard}:
 * it handles <em>both</em> failure modes in one place —
 *
 * <ul>
 *   <li>decode failure (from {@link ResilientLocationDeserializer}) → DLQ with the raw
 *       bytes + Kafka offset;</li>
 *   <li>decoded but structurally impossible (coord out of range, negative speed, …) → DLQ
 *       with a reason.</li>
 * </ul>
 *
 * Everything else flows on. Two counters (`ingest.valid`, `ingest.dlq`) make the split
 * visible in Prometheus.
 */
public class LocationIngest extends ProcessFunction<ParsedLocation, DriverLocation> {

    private transient Counter valid;
    private transient Counter dlq;

    @Override
    public void open(OpenContext ctx) {
        var group = getRuntimeContext().getMetricGroup().addGroup("flowfleet").addGroup("ingest");
        valid = group.counter("valid");
        dlq = group.counter("dlq");
    }

    @Override
    public void processElement(ParsedLocation p, Context ctx, Collector<DriverLocation> out) {
        if (!p.ok()) {
            dlq.inc();
            ctx.output(Tags.DLQ_LOCATIONS, decodeFailureJson(p));
            return;
        }
        String reason = validate(p.value);
        if (reason != null) {
            dlq.inc();
            ctx.output(Tags.DLQ_LOCATIONS, invalidJson(p.value, reason, p));
            return;
        }
        valid.inc();
        out.collect(p.value);
    }

    static String validate(DriverLocation loc) {
        if (!GeoPoint.isValid(loc.getLatitude(), loc.getLongitude())) {
            return "coordinate out of range";
        }
        if (loc.getSpeedKph() != null && (loc.getSpeedKph() < 0 || loc.getSpeedKph() > 400)) {
            return "impossible speed";
        }
        if (loc.getHeadingDegrees() != null
                && (loc.getHeadingDegrees() < 0 || loc.getHeadingDegrees() >= 360)) {
            return "heading out of range";
        }
        return null;
    }

    private static String decodeFailureJson(ParsedLocation p) {
        return "{\"reason\":\"decode-failure\",\"error\":" + quote(p.error)
                + ",\"topic\":" + quote(p.topic) + ",\"partition\":" + p.partition
                + ",\"offset\":" + p.offset + ",\"rawBase64\":" + quote(p.rawBase64) + "}";
    }

    private static String invalidJson(DriverLocation loc, String reason, ParsedLocation p) {
        return "{\"reason\":" + quote(reason) + ",\"driverId\":" + loc.getDriverId()
                + ",\"latitude\":" + loc.getLatitude() + ",\"longitude\":" + loc.getLongitude()
                + ",\"eventTime\":\"" + loc.getEventTime() + "\""
                + ",\"topic\":" + quote(p.topic) + ",\"partition\":" + p.partition
                + ",\"offset\":" + p.offset + "}";
    }

    private static String quote(String s) {
        return s == null ? "null" : "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}

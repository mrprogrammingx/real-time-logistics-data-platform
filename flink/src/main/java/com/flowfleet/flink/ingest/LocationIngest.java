package com.flowfleet.flink.ingest;

import com.flowfleet.common.domain.GeoPoint;
import com.flowfleet.events.avro.DriverLocation;
import com.flowfleet.flink.Tags;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
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
 * Everything else flows on. Two counters (`ingest.valid`, `ingest.dlq`) plus an
 * event-time-to-now latency histogram (`ingest.latencyMs`) make the split and the
 * pipeline lag visible in Prometheus — the headline signals for the Phase 8 load test.
 */
public class LocationIngest extends ProcessFunction<ParsedLocation, DriverLocation> {

    /** Samples kept for the latency percentiles. ~10k ≈ a few seconds at load. */
    private static final int LATENCY_WINDOW = 10_000;

    private transient Counter valid;
    private transient Counter dlq;
    private transient Histogram latencyMs;

    @Override
    public void open(OpenContext ctx) {
        var group = getRuntimeContext().getMetricGroup().addGroup("flowfleet").addGroup("ingest");
        valid = group.counter("valid");
        dlq = group.counter("dlq");
        latencyMs = group.histogram("latencyMs", new DescriptiveStatisticsHistogram(LATENCY_WINDOW));
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
        latencyMs.update(latencyMillis(p.value, System.currentTimeMillis()));
        out.collect(p.value);
    }

    /**
     * Milliseconds from the sample's event time to now. Clamped at 0: the device clock can
     * be a hair ahead of the Flink TM clock, and a negative "latency" is meaningless noise.
     */
    static long latencyMillis(DriverLocation loc, long nowMillis) {
        long delta = nowMillis - loc.getEventTime().toEpochMilli();
        return delta < 0 ? 0 : delta;
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

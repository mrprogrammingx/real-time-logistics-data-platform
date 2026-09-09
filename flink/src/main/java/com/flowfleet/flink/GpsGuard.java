package com.flowfleet.flink;

import com.flowfleet.common.domain.GeoPoint;
import com.flowfleet.events.avro.DriverLocation;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Splits the raw location stream: structurally sane samples flow on; the rest go to
 * {@link Tags#DLQ_LOCATIONS} as a JSON blob with the reason. Nothing is dropped.
 *
 * <p>(A sample so broken it can't even be Avro-decoded fails at the Kafka source instead;
 * that path is handled by the deserializer's error policy.)
 */
public class GpsGuard extends ProcessFunction<DriverLocation, DriverLocation> {

    @Override
    public void processElement(DriverLocation loc, Context ctx, Collector<DriverLocation> out) {
        String reason = validate(loc);
        if (reason == null) {
            out.collect(loc);
        } else {
            ctx.output(Tags.DLQ_LOCATIONS, toJson(loc, reason));
        }
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

    static String toJson(DriverLocation loc, String reason) {
        return "{\"reason\":\"" + reason + "\",\"driverId\":" + loc.getDriverId()
                + ",\"latitude\":" + loc.getLatitude() + ",\"longitude\":" + loc.getLongitude()
                + ",\"eventTime\":\"" + loc.getEventTime() + "\"}";
    }
}

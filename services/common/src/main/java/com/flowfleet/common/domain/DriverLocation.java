package com.flowfleet.common.domain;

import java.time.Instant;

/**
 * A single GPS sample from a driver's device — the platform's highest-volume event.
 *
 * <p>Deliberately permissive: {@code latitude}/{@code longitude} are raw doubles and
 * {@code speedKph}/{@code headingDegrees} are nullable. That serves two goals:
 * <ul>
 *   <li>malformed samples ({@code lat=999}, {@code driverId=null}) can be parsed and then
 *       routed to the dead-letter queue rather than failing deserialization;</li>
 *   <li>schema evolution: v1 of the Avro schema had no speed/heading, v2 added them as
 *       nullable fields, and this record models both.</li>
 * </ul>
 *
 * @param driverId       device owner; null is invalid but representable
 * @param latitude       WGS-84 latitude, unvalidated
 * @param longitude      WGS-84 longitude, unvalidated
 * @param speedKph       ground speed if the device reported it (v2+)
 * @param headingDegrees compass heading 0–359 if reported (v2+)
 * @param eventTime      device timestamp; the event-time basis for watermarks in Flink
 */
public record DriverLocation(
        Long driverId,
        double latitude,
        double longitude,
        Double speedKph,
        Integer headingDegrees,
        Instant eventTime) {

    /** True when the sample carries the v2 kinematic fields. */
    public boolean hasKinematics() {
        return speedKph != null && headingDegrees != null;
    }

    /** Structural validity check used by the ingestion job to split good vs. DLQ. */
    public boolean isValid() {
        return driverId != null
                && eventTime != null
                && GeoPoint.isValid(latitude, longitude)
                && (speedKph == null || (speedKph >= 0 && speedKph <= 300))
                && (headingDegrees == null || (headingDegrees >= 0 && headingDegrees < 360));
    }

    /** Throws if {@link #isValid()} is false; otherwise returns the validated point. */
    public GeoPoint toGeoPoint() {
        return new GeoPoint(latitude, longitude);
    }
}

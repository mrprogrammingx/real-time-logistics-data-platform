package com.flowfleet.common.domain;

/**
 * A validated WGS-84 coordinate.
 *
 * <p>Use this whenever a point is expected to be real (restaurant location, customer
 * address, delivery pickup/dropoff). Raw inbound GPS samples stay as loose {@code double}s
 * on {@link DriverLocation} so that malformed values can still be routed to the DLQ
 * instead of blowing up on construction.
 */
public record GeoPoint(double latitude, double longitude) {

    public GeoPoint {
        if (!isValid(latitude, longitude)) {
            throw new IllegalArgumentException(
                    "invalid coordinate: lat=%s lon=%s".formatted(latitude, longitude));
        }
    }

    public static boolean isValid(double latitude, double longitude) {
        return latitude >= -90.0 && latitude <= 90.0
                && longitude >= -180.0 && longitude <= 180.0
                && !Double.isNaN(latitude) && !Double.isNaN(longitude);
    }

    private static final double EARTH_RADIUS_M = 6_371_000.0;

    /** Great-circle distance in metres (haversine). Good enough for geofence proximity. */
    public double distanceMetersTo(GeoPoint other) {
        double dLat = Math.toRadians(other.latitude - latitude);
        double dLon = Math.toRadians(other.longitude - longitude);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(latitude)) * Math.cos(Math.toRadians(other.latitude))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return EARTH_RADIUS_M * 2 * Math.asin(Math.sqrt(a));
    }

    /** Initial bearing to {@code other}, in degrees clockwise from north (0–360). */
    public double bearingDegreesTo(GeoPoint other) {
        double lat1 = Math.toRadians(latitude);
        double lat2 = Math.toRadians(other.latitude);
        double dLon = Math.toRadians(other.longitude - longitude);
        double y = Math.sin(dLon) * Math.cos(lat2);
        double x = Math.cos(lat1) * Math.sin(lat2)
                - Math.sin(lat1) * Math.cos(lat2) * Math.cos(dLon);
        double brng = Math.toDegrees(Math.atan2(y, x));
        return (brng + 360.0) % 360.0;
    }

    /**
     * A point {@code meters} away from this one along the given {@code bearingDegrees}
     * (clockwise from north). Used by the location simulator to move a driver along a route.
     */
    public GeoPoint destination(double bearingDegrees, double meters) {
        double angular = meters / EARTH_RADIUS_M;
        double brng = Math.toRadians(bearingDegrees);
        double lat1 = Math.toRadians(latitude);
        double lon1 = Math.toRadians(longitude);
        double lat2 = Math.asin(Math.sin(lat1) * Math.cos(angular)
                + Math.cos(lat1) * Math.sin(angular) * Math.cos(brng));
        double lon2 = lon1 + Math.atan2(
                Math.sin(brng) * Math.sin(angular) * Math.cos(lat1),
                Math.cos(angular) - Math.sin(lat1) * Math.sin(lat2));
        double lon2Deg = ((Math.toDegrees(lon2) + 540.0) % 360.0) - 180.0;
        return new GeoPoint(Math.toDegrees(lat2), lon2Deg);
    }

    /** Move at most {@code meters} toward {@code target}; snaps to {@code target} if closer. */
    public GeoPoint moveToward(GeoPoint target, double meters) {
        double remaining = distanceMetersTo(target);
        if (remaining <= meters || remaining == 0.0) {
            return target;
        }
        return destination(bearingDegreesTo(target), meters);
    }
}

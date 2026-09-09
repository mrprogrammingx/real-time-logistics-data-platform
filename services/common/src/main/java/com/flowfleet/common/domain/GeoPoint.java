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

    /** Great-circle distance in metres (haversine). Good enough for geofence proximity. */
    public double distanceMetersTo(GeoPoint other) {
        double earthRadius = 6_371_000.0;
        double dLat = Math.toRadians(other.latitude - latitude);
        double dLon = Math.toRadians(other.longitude - longitude);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(latitude)) * Math.cos(Math.toRadians(other.latitude))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return earthRadius * 2 * Math.asin(Math.sqrt(a));
    }
}

package com.flowfleet.common.domain;

/**
 * A named area used to detect enter/exit transitions for drivers.
 *
 * <p>The geometry is carried as WKT (well-known text, e.g.
 * {@code POLYGON((lon lat, lon lat, ...))}) so the domain stays free of any spatial
 * library. PostGIS parses it on the write side; the Flink geofencing job parses it with
 * JTS on the read side.
 */
public record Geofence(
        Long id,
        String name,
        GeofenceType type,
        String polygonWkt) {

    public Geofence {
        if (polygonWkt == null || polygonWkt.isBlank()) {
            throw new IllegalArgumentException("polygonWkt is required");
        }
    }
}

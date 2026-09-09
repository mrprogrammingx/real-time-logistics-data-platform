package com.flowfleet.common.domain;

import java.time.Instant;

/**
 * The physical fulfilment of an {@link Order} by a {@link Driver}.
 *
 * <p>Pickup/dropoff are stored as plain doubles here to mirror the database columns; call
 * {@link #pickup()} / {@link #dropoff()} to get validated {@link GeoPoint}s.
 */
public record Delivery(
        Long id,
        Long orderId,
        Long driverId,
        DeliveryStatus status,
        double pickupLat,
        double pickupLon,
        double deliveryLat,
        double deliveryLon,
        Instant createdAt,
        Instant updatedAt) {

    public GeoPoint pickup() {
        return new GeoPoint(pickupLat, pickupLon);
    }

    public GeoPoint dropoff() {
        return new GeoPoint(deliveryLat, deliveryLon);
    }

    /** Straight-line pickup-to-customer distance in metres. */
    public double routeDistanceMeters() {
        return pickup().distanceMetersTo(dropoff());
    }
}

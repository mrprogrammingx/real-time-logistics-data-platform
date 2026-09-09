package com.flowfleet.common.domain;

/**
 * The catalogue of operational events the platform recognises.
 *
 * <p>Some are derived from PostgreSQL row changes (via Debezium CDC), others are emitted
 * directly as high-volume events by the simulator, and a few are produced by Flink itself
 * (for example {@link #ORDER_PICKUP_TIMEOUT}). Keeping the names in one enum makes the
 * routing rules in {@code kafka-design.md} unambiguous.
 */
public enum DomainEventType {
    // --- order / delivery lifecycle (mostly CDC-derived) ---
    ORDER_CREATED,
    ORDER_CONFIRMED,
    DRIVER_ASSIGNED,
    DRIVER_ARRIVED,
    ORDER_PICKED_UP,
    ORDER_DELIVERED,
    ORDER_CANCELLED,

    // --- driver movement (high-volume, simulator-emitted) ---
    DRIVER_LOCATION_UPDATED,
    GEOFENCE_ENTERED,
    GEOFENCE_EXITED,

    // --- shift lifecycle ---
    DRIVER_SHIFT_STARTED,
    DRIVER_SHIFT_ENDED,

    // --- Flink-derived alerts ---
    ORDER_PICKUP_TIMEOUT
}

package com.flowfleet.common.domain;

/**
 * Lifecycle of a delivery (the physical fulfilment of an order by a driver).
 *
 * <p>Separate from {@link OrderStatus}: one order has exactly one delivery today, but the
 * delivery timeline is what the geofencing and latency pipelines actually reason about.
 */
public enum DeliveryStatus {
    PENDING,
    DRIVER_ASSIGNED,
    EN_ROUTE_TO_PICKUP,
    AT_PICKUP,
    PICKED_UP,
    EN_ROUTE_TO_CUSTOMER,
    AT_CUSTOMER,
    DELIVERED,
    FAILED,
    CANCELLED;

    public boolean isTerminal() {
        return this == DELIVERED || this == FAILED || this == CANCELLED;
    }
}

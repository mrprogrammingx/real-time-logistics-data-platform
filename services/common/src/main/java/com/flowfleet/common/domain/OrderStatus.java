package com.flowfleet.common.domain;

import java.util.EnumSet;
import java.util.Set;

/**
 * Lifecycle of a customer order.
 *
 * <p>The allowed transitions form a small state machine. Downstream Flink jobs rely on this
 * ordering when they reconstruct an order's history from a stream of CDC events, so the
 * transitions are defined here once and reused everywhere.
 */
public enum OrderStatus {
    CREATED,
    CONFIRMED,
    PREPARING,
    READY_FOR_PICKUP,
    PICKED_UP,
    OUT_FOR_DELIVERY,
    DELIVERED,
    CANCELLED;

    private Set<OrderStatus> allowedNext() {
        return switch (this) {
            case CREATED -> EnumSet.of(CONFIRMED, CANCELLED);
            case CONFIRMED -> EnumSet.of(PREPARING, CANCELLED);
            case PREPARING -> EnumSet.of(READY_FOR_PICKUP, CANCELLED);
            case READY_FOR_PICKUP -> EnumSet.of(PICKED_UP, CANCELLED);
            case PICKED_UP -> EnumSet.of(OUT_FOR_DELIVERY, CANCELLED);
            case OUT_FOR_DELIVERY -> EnumSet.of(DELIVERED, CANCELLED);
            case DELIVERED, CANCELLED -> EnumSet.noneOf(OrderStatus.class);
        };
    }

    public boolean isTerminal() {
        return this == DELIVERED || this == CANCELLED;
    }

    public boolean canTransitionTo(OrderStatus next) {
        return allowedNext().contains(next);
    }
}

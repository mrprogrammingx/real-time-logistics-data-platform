package com.flowfleet.common.domain;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * A customer order.
 *
 * <p>This is the OLTP shape that PostgreSQL stores and Debezium streams. The
 * {@code totalAmount} is denormalised from {@link OrderItem}s for convenience; the API is
 * responsible for keeping it consistent.
 */
public record Order(
        Long id,
        Long customerId,
        Long restaurantId,
        OrderStatus status,
        BigDecimal totalAmount,
        Instant createdAt,
        Instant updatedAt) {

    public Order {
        if (totalAmount != null && totalAmount.signum() < 0) {
            throw new IllegalArgumentException("totalAmount must not be negative");
        }
    }

    /** Returns a copy with a new status, enforcing the {@link OrderStatus} state machine. */
    public Order withStatus(OrderStatus next, Instant now) {
        if (!status.canTransitionTo(next)) {
            throw new IllegalStateException("illegal order transition %s -> %s".formatted(status, next));
        }
        return new Order(id, customerId, restaurantId, next, totalAmount, createdAt, now);
    }
}

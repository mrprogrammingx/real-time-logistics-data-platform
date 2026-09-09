package com.flowfleet.api.order;

import java.math.BigDecimal;
import java.time.Instant;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.relational.core.mapping.Table;

/**
 * Persistence view of an order row.
 *
 * <p>Order items are a <em>separate</em> aggregate ({@link OrderItemRow}), not a
 * {@code @MappedCollection} here, so that a status change on the order never deletes and
 * re-inserts its items. Stable item primary keys matter once Debezium is streaming this
 * table.
 */
@Table("orders")
record OrderRow(
        @Id Long id,
        Long customerId,
        Long restaurantId,
        String status,
        BigDecimal totalAmount,
        @Version Long version,
        Instant createdAt,
        Instant updatedAt) {

    static OrderRow newOrder(Long customerId, Long restaurantId, String status,
                             BigDecimal totalAmount, Instant now) {
        return new OrderRow(null, customerId, restaurantId, status, totalAmount, null, now, now);
    }

    OrderRow withStatus(String newStatus, Instant now) {
        return new OrderRow(id, customerId, restaurantId, newStatus, totalAmount, version, createdAt, now);
    }
}

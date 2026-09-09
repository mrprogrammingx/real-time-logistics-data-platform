package com.flowfleet.common.domain;

import java.math.BigDecimal;

/** A single line on an {@link Order}. */
public record OrderItem(
        Long id,
        Long orderId,
        String name,
        int quantity,
        BigDecimal unitPrice) {

    public OrderItem {
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be positive");
        }
        if (unitPrice == null || unitPrice.signum() < 0) {
            throw new IllegalArgumentException("unitPrice must be present and non-negative");
        }
    }

    public BigDecimal lineTotal() {
        return unitPrice.multiply(BigDecimal.valueOf(quantity));
    }
}

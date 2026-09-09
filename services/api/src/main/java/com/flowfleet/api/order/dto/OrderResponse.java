package com.flowfleet.api.order.dto;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public record OrderResponse(
        Long id,
        Long customerId,
        Long restaurantId,
        String status,
        BigDecimal totalAmount,
        Long version,
        Instant createdAt,
        Instant updatedAt,
        List<OrderItemResponse> items) {
}

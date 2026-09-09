package com.flowfleet.api.order.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.util.List;

public record CreateOrderRequest(
        @NotNull Long customerId,
        @NotNull Long restaurantId,
        @NotEmpty @Valid List<OrderItemRequest> items) {
}

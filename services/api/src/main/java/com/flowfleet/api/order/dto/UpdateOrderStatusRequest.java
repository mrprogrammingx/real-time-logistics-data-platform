package com.flowfleet.api.order.dto;

import com.flowfleet.common.domain.OrderStatus;
import jakarta.validation.constraints.NotNull;

public record UpdateOrderStatusRequest(@NotNull OrderStatus status) {
}

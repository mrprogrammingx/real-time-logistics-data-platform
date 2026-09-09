package com.flowfleet.api.order.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import java.math.BigDecimal;

public record OrderItemRequest(
        @NotBlank String name,
        @Positive int quantity,
        @NotNull @PositiveOrZero BigDecimal unitPrice) {
}

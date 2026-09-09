package com.flowfleet.api.order;

import java.math.BigDecimal;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Table("order_items")
record OrderItemRow(
        @Id Long id,
        Long orderId,
        String name,
        int quantity,
        BigDecimal unitPrice) {

    BigDecimal lineTotal() {
        return unitPrice.multiply(BigDecimal.valueOf(quantity));
    }
}

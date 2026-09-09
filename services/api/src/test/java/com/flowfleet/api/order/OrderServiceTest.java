package com.flowfleet.api.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.flowfleet.api.web.IllegalStateTransitionException;
import com.flowfleet.common.domain.OrderStatus;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    private static final Clock FIXED = Clock.fixed(Instant.parse("2026-09-09T12:00:00Z"), ZoneOffset.UTC);

    @Mock OrderRepository orders;
    @Mock OrderItemRepository orderItems;

    private OrderService service;

    @BeforeEach
    void setUp() {
        service = new OrderService(orders, orderItems, FIXED);
    }

    private OrderRow row(OrderStatus status) {
        return new OrderRow(7L, 1L, 1L, status.name(), new BigDecimal("5.00"), 3L,
                FIXED.instant(), FIXED.instant());
    }

    @Test
    void changeStatusRejectsIllegalTransition() {
        when(orders.findById(7L)).thenReturn(Optional.of(row(OrderStatus.CREATED)));

        assertThatThrownBy(() -> service.changeStatus(7L, OrderStatus.DELIVERED))
                .isInstanceOf(IllegalStateTransitionException.class);

        verify(orders, never()).save(any());
    }

    @Test
    void changeStatusToSameStatusIsANoOp() {
        when(orders.findById(7L)).thenReturn(Optional.of(row(OrderStatus.CONFIRMED)));
        when(orderItems.findByOrderId(7L)).thenReturn(List.of());

        var result = service.changeStatus(7L, OrderStatus.CONFIRMED);

        assertThat(result.status()).isEqualTo("CONFIRMED");
        verify(orders, never()).save(any());
    }

    @Test
    void changeStatusPersistsLegalTransition() {
        when(orders.findById(7L)).thenReturn(Optional.of(row(OrderStatus.CONFIRMED)));
        when(orderItems.findByOrderId(7L)).thenReturn(List.of());
        when(orders.save(any())).thenAnswer(inv -> inv.getArgument(0));

        var result = service.changeStatus(7L, OrderStatus.PREPARING);

        assertThat(result.status()).isEqualTo("PREPARING");
        assertThat(result.updatedAt()).isEqualTo(FIXED.instant());
        verify(orders).save(any(OrderRow.class));
    }

    @Test
    void getUnknownOrderThrows() {
        when(orders.findById(anyLong())).thenReturn(Optional.empty());
        assertThatThrownBy(() -> service.get(123L)).hasMessageContaining("not found");
    }
}

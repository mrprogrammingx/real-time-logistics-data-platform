package com.flowfleet.api.order;

import com.flowfleet.api.order.dto.CreateOrderRequest;
import com.flowfleet.api.order.dto.OrderItemRequest;
import com.flowfleet.api.order.dto.OrderItemResponse;
import com.flowfleet.api.order.dto.OrderResponse;
import com.flowfleet.api.web.IllegalStateTransitionException;
import com.flowfleet.api.web.NotFoundException;
import com.flowfleet.common.domain.OrderStatus;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
class OrderService {

    private final OrderRepository orders;
    private final OrderItemRepository orderItems;
    private final Clock clock;

    OrderService(OrderRepository orders, OrderItemRepository orderItems, Clock clock) {
        this.orders = orders;
        this.orderItems = orderItems;
        this.clock = clock;
    }

    @Transactional
    OrderResponse create(CreateOrderRequest request) {
        Instant now = clock.instant();
        BigDecimal total = request.items().stream()
                .map(i -> i.unitPrice().multiply(BigDecimal.valueOf(i.quantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        OrderRow saved = orders.save(OrderRow.newOrder(
                request.customerId(), request.restaurantId(), OrderStatus.CREATED.name(), total, now));

        List<OrderItemRow> items = request.items().stream()
                .map(i -> new OrderItemRow(null, saved.id(), i.name(), i.quantity(), i.unitPrice()))
                .toList();
        List<OrderItemRow> persisted = orderItems.saveAll(items);

        return toResponse(saved, persisted);
    }

    @Transactional(readOnly = true)
    OrderResponse get(long id) {
        OrderRow row = orders.findById(id).orElseThrow(() -> new NotFoundException("order", id));
        return toResponse(row, orderItems.findByOrderId(id));
    }

    @Transactional(readOnly = true)
    List<OrderResponse> list(OrderStatus status) {
        List<OrderRow> rows = status == null ? orders.findAll() : orders.findByStatus(status.name());
        return rows.stream()
                .map(r -> toResponse(r, orderItems.findByOrderId(r.id())))
                .toList();
    }

    @Transactional
    OrderResponse changeStatus(long id, OrderStatus target) {
        OrderRow row = orders.findById(id).orElseThrow(() -> new NotFoundException("order", id));
        OrderStatus current = OrderStatus.valueOf(row.status());
        if (current == target) {
            return toResponse(row, orderItems.findByOrderId(id));
        }
        if (!current.canTransitionTo(target)) {
            throw new IllegalStateTransitionException(current, target);
        }
        OrderRow updated = orders.save(row.withStatus(target.name(), clock.instant()));
        return toResponse(updated, orderItems.findByOrderId(id));
    }

    private static OrderResponse toResponse(OrderRow row, List<OrderItemRow> items) {
        List<OrderItemResponse> itemResponses = items.stream()
                .map(i -> new OrderItemResponse(i.id(), i.name(), i.quantity(), i.unitPrice(), i.lineTotal()))
                .toList();
        return new OrderResponse(
                row.id(), row.customerId(), row.restaurantId(), row.status(), row.totalAmount(),
                row.version(), row.createdAt(), row.updatedAt(), itemResponses);
    }
}

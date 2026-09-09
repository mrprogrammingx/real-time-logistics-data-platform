package com.flowfleet.api.order;

import com.flowfleet.api.order.dto.CreateOrderRequest;
import com.flowfleet.api.order.dto.OrderResponse;
import com.flowfleet.api.order.dto.UpdateOrderStatusRequest;
import com.flowfleet.common.domain.OrderStatus;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/orders")
class OrderController {

    private final OrderService orders;

    OrderController(OrderService orders) {
        this.orders = orders;
    }

    @PostMapping
    ResponseEntity<OrderResponse> create(@Valid @RequestBody CreateOrderRequest request) {
        OrderResponse created = orders.create(request);
        return ResponseEntity.created(URI.create("/api/v1/orders/" + created.id())).body(created);
    }

    @GetMapping("/{id}")
    OrderResponse get(@PathVariable long id) {
        return orders.get(id);
    }

    @GetMapping
    List<OrderResponse> list(@RequestParam(required = false) OrderStatus status) {
        return orders.list(status);
    }

    @PatchMapping("/{id}/status")
    OrderResponse changeStatus(@PathVariable long id, @Valid @RequestBody UpdateOrderStatusRequest request) {
        return orders.changeStatus(id, request.status());
    }
}

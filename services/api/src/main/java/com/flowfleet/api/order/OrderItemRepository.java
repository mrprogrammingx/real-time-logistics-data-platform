package com.flowfleet.api.order;

import java.util.List;
import org.springframework.data.repository.ListCrudRepository;

interface OrderItemRepository extends ListCrudRepository<OrderItemRow, Long> {

    List<OrderItemRow> findByOrderId(Long orderId);
}

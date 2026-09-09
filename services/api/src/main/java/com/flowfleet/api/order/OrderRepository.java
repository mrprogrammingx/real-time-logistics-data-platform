package com.flowfleet.api.order;

import java.util.List;
import org.springframework.data.repository.ListCrudRepository;

interface OrderRepository extends ListCrudRepository<OrderRow, Long> {

    List<OrderRow> findByStatus(String status);
}

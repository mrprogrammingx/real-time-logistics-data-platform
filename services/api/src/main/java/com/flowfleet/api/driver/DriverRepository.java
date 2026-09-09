package com.flowfleet.api.driver;

import java.util.List;
import org.springframework.data.repository.ListCrudRepository;

interface DriverRepository extends ListCrudRepository<DriverRow, Long> {

    List<DriverRow> findByStatus(String status);
}

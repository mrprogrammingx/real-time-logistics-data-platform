package com.flowfleet.api.driver;

import java.time.Instant;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.relational.core.mapping.Table;

@Table("drivers")
record DriverRow(
        @Id Long id,
        String name,
        String status,
        Long vehicleId,
        @Version Long version,
        Instant createdAt,
        Instant updatedAt) {

    static DriverRow newDriver(String name, String status, Long vehicleId, Instant now) {
        return new DriverRow(null, name, status, vehicleId, null, now, now);
    }

    DriverRow withStatus(String newStatus, Instant now) {
        return new DriverRow(id, name, newStatus, vehicleId, version, createdAt, now);
    }
}

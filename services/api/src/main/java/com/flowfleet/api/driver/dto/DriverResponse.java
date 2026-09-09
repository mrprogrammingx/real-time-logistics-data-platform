package com.flowfleet.api.driver.dto;

import java.time.Instant;

public record DriverResponse(
        Long id,
        String name,
        String status,
        Long vehicleId,
        Long version,
        Instant createdAt,
        Instant updatedAt) {
}

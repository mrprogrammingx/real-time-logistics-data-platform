package com.flowfleet.common.domain;

import java.time.Instant;

/** A vehicle assigned to a driver. */
public record Vehicle(
        Long id,
        VehicleType type,
        String plate,
        Instant createdAt) {
}

package com.flowfleet.common.domain;

import java.time.Instant;

/** A delivery driver. Partition key for every driver-scoped Kafka topic is {@code id}. */
public record Driver(
        Long id,
        String name,
        DriverStatus status,
        Long vehicleId,
        Instant createdAt,
        Instant updatedAt) {

    public Driver withStatus(DriverStatus next, Instant now) {
        return new Driver(id, name, next, vehicleId, createdAt, now);
    }
}

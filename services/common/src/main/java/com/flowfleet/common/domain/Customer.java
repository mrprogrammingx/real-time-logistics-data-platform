package com.flowfleet.common.domain;

import java.time.Instant;

/** A person who places orders. Becomes {@code dim_customer} in the warehouse. */
public record Customer(
        Long id,
        String name,
        String email,
        String phone,
        Instant createdAt) {
}

package com.flowfleet.common.domain;

import java.time.Duration;
import java.time.Instant;

/** A driver's working session, from clock-in to clock-out. */
public record DriverShift(
        Long id,
        Long driverId,
        ShiftStatus status,
        Instant startedAt,
        Instant endedAt) {

    public DriverShift {
        if (endedAt != null && startedAt != null && endedAt.isBefore(startedAt)) {
            throw new IllegalArgumentException("shift endedAt is before startedAt");
        }
    }

    /** Elapsed shift time; uses {@code now} while the shift is still open. */
    public Duration duration(Instant now) {
        Instant end = endedAt != null ? endedAt : now;
        return Duration.between(startedAt, end);
    }
}

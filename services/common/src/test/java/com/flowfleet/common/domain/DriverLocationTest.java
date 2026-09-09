package com.flowfleet.common.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class DriverLocationTest {

    private static final Instant NOW = Instant.parse("2026-09-09T10:00:00Z");

    @Test
    void v1SampleWithoutKinematicsIsValid() {
        DriverLocation loc = new DriverLocation(42L, 40.17, 44.50, null, null, NOW);
        assertThat(loc.isValid()).isTrue();
        assertThat(loc.hasKinematics()).isFalse();
    }

    @Test
    void v2SampleWithKinematicsIsValid() {
        DriverLocation loc = new DriverLocation(42L, 40.17, 44.50, 32.4, 174, NOW);
        assertThat(loc.isValid()).isTrue();
        assertThat(loc.hasKinematics()).isTrue();
    }

    @Test
    void malformedSamplesAreParseableButInvalid() {
        assertThat(new DriverLocation(null, 40.0, 44.0, null, null, NOW).isValid()).isFalse();
        assertThat(new DriverLocation(1L, 999.0, 200.0, null, null, NOW).isValid()).isFalse();
        assertThat(new DriverLocation(1L, 40.0, 44.0, -5.0, null, NOW).isValid()).isFalse();
        assertThat(new DriverLocation(1L, 40.0, 44.0, null, 400, NOW).isValid()).isFalse();
        assertThat(new DriverLocation(1L, 40.0, 44.0, null, null, null).isValid()).isFalse();
    }
}

package com.flowfleet.common.domain;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class GeoPointMovementTest {

    private static final GeoPoint REPUBLIC_SQ = new GeoPoint(40.1776, 44.5126);
    private static final GeoPoint OPERA = new GeoPoint(40.1872, 44.5153);

    @Test
    void destinationThenDistanceRoundTrips() {
        GeoPoint moved = REPUBLIC_SQ.destination(45, 500);
        assertThat(REPUBLIC_SQ.distanceMetersTo(moved)).isCloseTo(500, org.assertj.core.data.Offset.offset(1.0));
    }

    @Test
    void bearingNorthIsZero() {
        GeoPoint north = REPUBLIC_SQ.destination(0, 300);
        assertThat(north.latitude()).isGreaterThan(REPUBLIC_SQ.latitude());
        assertThat(REPUBLIC_SQ.bearingDegreesTo(north)).isCloseTo(0, org.assertj.core.data.Offset.offset(0.5));
    }

    @Test
    void moveTowardSnapsWhenWithinReach() {
        assertThat(REPUBLIC_SQ.moveToward(OPERA, 10_000_000)).isEqualTo(OPERA);
    }

    @Test
    void moveTowardAdvancesAlongTheBearing() {
        double total = REPUBLIC_SQ.distanceMetersTo(OPERA);
        GeoPoint step = REPUBLIC_SQ.moveToward(OPERA, 200);
        assertThat(REPUBLIC_SQ.distanceMetersTo(step)).isCloseTo(200, org.assertj.core.data.Offset.offset(1.0));
        assertThat(step.distanceMetersTo(OPERA)).isLessThan(total);
    }
}

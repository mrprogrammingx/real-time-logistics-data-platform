package com.flowfleet.common.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class GeoPointTest {

    @Test
    void rejectsOutOfRangeCoordinates() {
        assertThatThrownBy(() -> new GeoPoint(999, 200))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new GeoPoint(0, Double.NaN))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void acceptsValidCoordinates() {
        GeoPoint yerevan = new GeoPoint(40.1772, 44.5035);
        assertThat(yerevan.latitude()).isEqualTo(40.1772);
    }

    @Test
    void haversineDistanceIsApproximatelyCorrect() {
        // Yerevan Republic Square -> Yerevan Opera, ~1.4 km apart.
        GeoPoint a = new GeoPoint(40.1776, 44.5126);
        GeoPoint b = new GeoPoint(40.1872, 44.5153);
        assertThat(a.distanceMetersTo(b)).isBetween(1_000.0, 1_300.0);
    }

    @Test
    void distanceToSelfIsZero() {
        GeoPoint a = new GeoPoint(40.0, 44.0);
        assertThat(a.distanceMetersTo(a)).isEqualTo(0.0);
    }
}

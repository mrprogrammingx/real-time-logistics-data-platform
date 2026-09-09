package com.flowfleet.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import com.flowfleet.consumer.stats.DriverSpeedStats;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

class DriverSpeedStatsTest {

    @Test
    void runningMeanIsCorrect() {
        var stats = new DriverSpeedStats();
        stats.record(1L, 10.0);
        stats.record(1L, 20.0);
        stats.record(1L, 30.0);

        assertThat(stats.meanKph(1L)).isCloseTo(20.0, Offset.offset(1e-9));
        assertThat(stats.processed()).isEqualTo(3);
        assertThat(stats.driversSeen()).isEqualTo(1);
    }

    @Test
    void nullSpeedStillCountsAsProcessedButNotTowardMean() {
        var stats = new DriverSpeedStats();
        stats.record(2L, null);
        stats.record(2L, 40.0);

        assertThat(stats.processed()).isEqualTo(2);
        assertThat(stats.meanKph(2L)).isCloseTo(40.0, Offset.offset(1e-9));
    }

    @Test
    void unknownDriverIsNaN() {
        assertThat(new DriverSpeedStats().meanKph(99L)).isNaN();
    }
}

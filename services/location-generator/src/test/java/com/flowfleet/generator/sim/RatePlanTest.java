package com.flowfleet.generator.sim;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class RatePlanTest {

    @Test
    void wholeNumberRateGivesConstantCount() {
        RatePlan plan = new RatePlan();
        for (int i = 0; i < 20; i++) {
            assertThat(plan.eventsThisTick(10_000, 100)).isEqualTo(1_000);
        }
    }

    @Test
    void fractionalRateAveragesOutExactlyOverTime() {
        RatePlan plan = new RatePlan();
        long total = 0;
        int ticks = 300;                      // 30 s of 100 ms ticks
        for (int i = 0; i < ticks; i++) {
            total += plan.eventsThisTick(3_333, 100);
        }
        // 3333 ev/s * 30 s = 99_990, carry never lets it drift more than 1
        assertThat(total).isBetween(99_989L, 99_990L);
    }

    @Test
    void rateCanChangeBetweenTicksWithoutLosingTheCarry() {
        RatePlan plan = new RatePlan();
        long total = plan.eventsThisTick(1_500, 100)      // 150
                + plan.eventsThisTick(1_500, 100)         // 150
                + plan.eventsThisTick(25_000, 100)        // 2500
                + plan.eventsThisTick(25_000, 100);       // 2500
        assertThat(total).isEqualTo(5_300);
    }

    @Test
    void negativeRateIsClampedToZero() {
        assertThat(new RatePlan().eventsThisTick(-5, 100)).isZero();
    }
}

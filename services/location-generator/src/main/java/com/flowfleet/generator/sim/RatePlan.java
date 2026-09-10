package com.flowfleet.generator.sim;

/**
 * Converts a target throughput (events/second) into an integer number of events to emit on
 * each fixed-length tick, carrying the fractional remainder so the long-run average is
 * exact (e.g. 3333 ev/s on a 200 ms tick alternates 666/667/667 rather than always 666).
 *
 * <p>The target rate is passed to {@link #eventsThisTick} each call so a load ramp can
 * change it between ticks without resetting the carry.
 */
public final class RatePlan {

    private double carry;

    /** Events to emit on a tick of {@code tickMillis} at {@code ratePerSecond}. Never negative. */
    public int eventsThisTick(double ratePerSecond, long tickMillis) {
        double want = Math.max(0.0, ratePerSecond) * tickMillis / 1000.0 + carry;
        int n = (int) Math.floor(want);
        carry = want - n;
        return n;
    }

    double carry() {
        return carry;
    }
}

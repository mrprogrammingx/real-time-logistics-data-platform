package com.flowfleet.consumer.stats;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

/**
 * Trivial in-memory aggregation so the consumer has real per-record work: a running mean
 * speed per driver, plus a processed count. Not persisted — it exists to give the
 * rebalance experiment something observable and to exercise deserialization.
 */
@Component
public class DriverSpeedStats {

    private record Running(long count, double meanKph) {
        Running plus(double sample) {
            long n = count + 1;
            return new Running(n, meanKph + (sample - meanKph) / n);
        }
    }

    private final Map<Long, Running> byDriver = new ConcurrentHashMap<>();
    private volatile long processed = 0;

    public void record(long driverId, Double speedKph) {
        if (speedKph != null) {
            byDriver.merge(driverId, new Running(1, speedKph),
                    (cur, add) -> cur.plus(add.meanKph()));
        }
        processed++;
    }

    public long processed() {
        return processed;
    }

    public int driversSeen() {
        return byDriver.size();
    }

    public double meanKph(long driverId) {
        Running r = byDriver.get(driverId);
        return r == null ? Double.NaN : r.meanKph();
    }
}

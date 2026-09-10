package com.flowfleet.flink.sink;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * Identity map that sleeps {@code delayMs} per record — the deliberately-slow sink for the
 * Phase 6 backpressure experiment. {@code delayMs <= 0} is a no-op (the normal case).
 * Enable with {@code --slow-map-ms 20}.
 */
public class SlowMap<T> implements MapFunction<T, T> {

    private final long delayMs;

    public SlowMap(long delayMs) {
        this.delayMs = delayMs;
    }

    @Override
    public T map(T value) throws InterruptedException {
        if (delayMs > 0) {
            Thread.sleep(delayMs);
        }
        return value;
    }
}

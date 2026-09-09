package com.flowfleet.flink;

import com.flowfleet.events.avro.DriverLocation;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

/**
 * Event-time is the driver device's timestamp ({@code DriverLocation.eventTime}), not the
 * time Flink saw the record — a sample buffered on a phone for a few seconds must still
 * land in the window it belongs to.
 *
 * <ul>
 *   <li>{@code forBoundedOutOfOrderness(5s)} — tolerate reordering up to 5 seconds;</li>
 *   <li>{@code withIdleness(30s)} — a partition with no traffic must not hold the whole
 *       job's watermark back.</li>
 * </ul>
 *
 * Samples later than the watermark are <em>not</em> dropped — the windowed jobs route them
 * to a late-data side output ({@link Tags#LATE_LOCATIONS}).
 */
public final class Watermarks {

    public static final Duration OUT_OF_ORDERNESS = Duration.ofSeconds(5);
    public static final Duration IDLENESS = Duration.ofSeconds(30);

    private Watermarks() {}

    public static WatermarkStrategy<DriverLocation> forDriverLocation() {
        return WatermarkStrategy
                .<DriverLocation>forBoundedOutOfOrderness(OUT_OF_ORDERNESS)
                .withTimestampAssigner((loc, recordTs) -> loc.getEventTime().toEpochMilli())
                .withIdleness(IDLENESS);
    }
}

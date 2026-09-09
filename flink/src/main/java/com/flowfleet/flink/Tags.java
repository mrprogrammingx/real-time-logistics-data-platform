package com.flowfleet.flink;

import com.flowfleet.events.avro.DriverLocation;
import org.apache.flink.util.OutputTag;

/**
 * Side-output tags. Bad records and late records are never silently dropped — they go to a
 * tagged stream that a job routes to a DLQ / a metric.
 */
public final class Tags {

    private Tags() {}

    /** Structurally invalid GPS samples, as a JSON blob with the reason. → {@code *.dlq}. */
    public static final OutputTag<String> DLQ_LOCATIONS = new OutputTag<>("dlq-locations") {};

    /** Samples that arrived after their window's watermark. */
    public static final OutputTag<DriverLocation> LATE_LOCATIONS = new OutputTag<>("late-locations") {};
}

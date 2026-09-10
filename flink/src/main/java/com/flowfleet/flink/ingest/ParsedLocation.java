package com.flowfleet.flink.ingest;

import com.flowfleet.events.avro.DriverLocation;

/**
 * The outcome of trying to decode one Kafka record from {@code flowfleet.driver.locations}.
 *
 * <p>Either {@link #value} is set (decode succeeded), or {@link #error} is set (the bytes
 * weren't valid Confluent-Avro, or the schema id was unknown). A failed decode is
 * <em>never</em> allowed to throw out of the deserializer — that would fail the whole job
 * on one poison message. It carries its Kafka coordinates so the DLQ record is actionable.
 */
public class ParsedLocation {

    public DriverLocation value;   // null on decode failure

    public String error;           // null on success
    public String rawBase64;       // the undecodable bytes, for inspection
    public String topic;
    public int partition;
    public long offset;
    public long timestamp;

    public ParsedLocation() {}

    public boolean ok() {
        return value != null;
    }

    public static ParsedLocation of(DriverLocation value) {
        ParsedLocation p = new ParsedLocation();
        p.value = value;
        return p;
    }
}

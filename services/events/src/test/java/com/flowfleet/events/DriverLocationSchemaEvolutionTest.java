package com.flowfleet.events;

import static org.assertj.core.api.Assertions.assertThat;

import com.flowfleet.events.avro.DriverLocation;
import java.io.InputStream;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.junit.jupiter.api.Test;

/**
 * Proves the {@code DriverLocation} v1 -> v2 evolution is safe under Confluent Schema
 * Registry's {@code FULL} compatibility rule, so that during a rolling upgrade producers
 * and consumers on either schema version interoperate.
 *
 * <p>v2 adds {@code speedKph}, {@code headingDegrees}, {@code vehicleType} — all
 * {@code ["null", T]} with {@code default: null}. That default is what makes the change
 * both BACKWARD (v2 reader fills the default when reading v1 data) and FORWARD (v1 reader
 * ignores the unknown fields in v2 data).
 */
class DriverLocationSchemaEvolutionTest {

    private static final Schema V2 = DriverLocation.getClassSchema();
    private static final Schema V1 = load("/avro-history/driver-location.v1.avsc");

    private static Schema load(String resource) {
        try (InputStream in = DriverLocationSchemaEvolutionTest.class.getResourceAsStream(resource)) {
            return new Schema.Parser().parse(in);
        } catch (Exception e) {
            throw new IllegalStateException("cannot load " + resource, e);
        }
    }

    private static SchemaCompatibilityType readerWriter(Schema reader, Schema writer) {
        return SchemaCompatibility.checkReaderWriterCompatibility(reader, writer).getType();
    }

    @Test
    void v2ReaderCanReadV1Data_backward() {
        assertThat(readerWriter(V2, V1)).isEqualTo(SchemaCompatibilityType.COMPATIBLE);
    }

    @Test
    void v1ReaderCanReadV2Data_forward() {
        assertThat(readerWriter(V1, V2)).isEqualTo(SchemaCompatibilityType.COMPATIBLE);
    }

    @Test
    void evolutionIsFullyCompatible() {
        assertThat(readerWriter(V2, V1)).isEqualTo(SchemaCompatibilityType.COMPATIBLE);
        assertThat(readerWriter(V1, V2)).isEqualTo(SchemaCompatibilityType.COMPATIBLE);
    }

    @Test
    void addingARequiredFieldWithoutDefaultIsABreakingChange() {
        Schema v3Bad = new Schema.Parser().parse("""
            {
              "type": "record", "name": "DriverLocation", "namespace": "com.flowfleet.events.avro",
              "fields": [
                {"name": "driverId", "type": "long"},
                {"name": "latitude", "type": "double"},
                {"name": "longitude", "type": "double"},
                {"name": "eventTime", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "deviceId", "type": "string"}
              ]
            }
            """);
        // a v3-bad reader cannot read data written by v2 (no deviceId, no default)
        assertThat(readerWriter(v3Bad, V2)).isEqualTo(SchemaCompatibilityType.INCOMPATIBLE);
    }
}

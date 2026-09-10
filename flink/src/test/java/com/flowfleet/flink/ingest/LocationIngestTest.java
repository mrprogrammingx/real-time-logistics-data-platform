package com.flowfleet.flink.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import com.flowfleet.events.avro.DriverLocation;
import com.flowfleet.flink.Locations;
import com.flowfleet.flink.Tags;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LocationIngestTest {

    private OneInputStreamOperatorTestHarness<ParsedLocation, DriverLocation> harness;

    @BeforeEach
    void setUp() throws Exception {
        harness = ProcessFunctionTestHarnesses.forProcessFunction(new LocationIngest());
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    private List<String> dlq() {
        var q = harness.getSideOutput(Tags.DLQ_LOCATIONS);
        List<String> out = new ArrayList<>();
        if (q != null) {
            q.forEach(o -> out.add(((StreamRecord<String>) o).getValue()));
        }
        return out;
    }

    @Test
    void validSamplePassesThrough() throws Exception {
        harness.processElement(new StreamRecord<>(ParsedLocation.of(Locations.at(7, 40.17, 44.50, 1_000))));

        assertThat(harness.extractOutputValues()).singleElement()
                .satisfies(l -> assertThat(l.getDriverId()).isEqualTo(7L));
        assertThat(dlq()).isEmpty();
    }

    @Test
    void decodeFailureGoesToDlqWithKafkaCoordinates() throws Exception {
        ParsedLocation bad = new ParsedLocation();
        bad.error = "SerializationException: unknown magic byte";
        bad.topic = "flowfleet.driver.locations";
        bad.partition = 3;
        bad.offset = 4242;
        bad.rawBase64 = "AAAA";

        harness.processElement(new StreamRecord<>(bad));

        assertThat(harness.extractOutputValues()).isEmpty();
        assertThat(dlq()).singleElement().satisfies(json -> {
            assertThat(json).contains("\"reason\":\"decode-failure\"");
            assertThat(json).contains("\"partition\":3");
            assertThat(json).contains("\"offset\":4242");
            assertThat(json).contains("unknown magic byte");
        });
    }

    @Test
    void structurallyInvalidSamplesGoToDlqWithAReason() throws Exception {
        harness.processElement(new StreamRecord<>(ParsedLocation.of(Locations.at(1, 999.0, 44.0, 1_000))));
        harness.processElement(new StreamRecord<>(ParsedLocation.of(
                Locations.at(2, 40.0, 44.0, 2_000, /* speed */ -5.0, 90))));

        assertThat(harness.extractOutputValues()).isEmpty();
        assertThat(dlq()).hasSize(2);
        assertThat(dlq().get(0)).contains("coordinate out of range");
        assertThat(dlq().get(1)).contains("impossible speed");
    }

    @Test
    void latencyMillisIsEventTimeToNowClampedAtZero() {
        // sample stamped 2 s ago -> ~2000 ms latency
        DriverLocation twoSecondsOld = Locations.at(1, 40.0, 44.0, 10_000);
        assertThat(LocationIngest.latencyMillis(twoSecondsOld, 12_000)).isEqualTo(2_000);
        // device clock ahead of the TM clock -> clamp, never negative
        assertThat(LocationIngest.latencyMillis(twoSecondsOld, 9_500)).isZero();
    }

    @Test
    void countersReflectTheSplit() throws Exception {
        harness.processElement(new StreamRecord<>(ParsedLocation.of(Locations.at(1, 40.0, 44.0, 1_000))));
        harness.processElement(new StreamRecord<>(ParsedLocation.of(Locations.at(1, 40.0, 44.0, 2_000))));
        ParsedLocation bad = new ParsedLocation();
        bad.error = "boom";
        harness.processElement(new StreamRecord<>(bad));

        assertThat(harness.getOutput()).isNotNull();
        assertThat(harness.extractOutputValues()).hasSize(2);
        assertThat(dlq()).hasSize(1);
    }
}

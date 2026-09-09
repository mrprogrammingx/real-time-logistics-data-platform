package com.flowfleet.flink.geofence;

import static org.assertj.core.api.Assertions.assertThat;

import com.flowfleet.events.avro.DriverLocation;
import com.flowfleet.events.avro.GeofenceEvent;
import com.flowfleet.events.avro.GeofenceTransition;
import com.flowfleet.flink.Locations;
import com.flowfleet.flink.geo.Geofence;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithKeyedOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GeofenceFunctionTest {

    private static final long T0 = 1_800_000_000_000L;
    // ~120 m box around Republic Square
    private static final Geofence FENCE = new Geofence(1, "Republic Square", "RESTAURANT",
            "POLYGON((44.5126 40.1770, 44.5140 40.1770, 44.5140 40.1782, 44.5126 40.1782, 44.5126 40.1770))");

    private KeyedTwoInputStreamOperatorTestHarness<Long, DriverLocation, Geofence, GeofenceEvent> harness;

    @BeforeEach
    void setUp() throws Exception {
        harness = new KeyedTwoInputStreamOperatorTestHarness<>(
                new CoBroadcastWithKeyedOperator<>(new GeofenceFunction(), List.of(GeofenceFunction.FENCES)),
                DriverLocation::getDriverId, null, Types.LONG);
        harness.open();
        harness.processElement2(new StreamRecord<>(FENCE));   // broadcast the fence first
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    private void push(DriverLocation loc) throws Exception {
        harness.processElement1(new StreamRecord<>(loc, loc.getEventTime().toEpochMilli()));
    }

    private List<GeofenceEvent> events() {
        return harness.extractOutputValues();
    }

    @Test
    void emitsEnterThenExitAndNothingInBetween() throws Exception {
        push(Locations.at(7, 40.1900, 44.5133, T0));            // outside (north)
        push(Locations.at(7, 40.1776, 44.5133, T0 + 1000));     // inside  -> ENTER
        push(Locations.at(7, 40.1777, 44.5134, T0 + 2000));     // still inside -> nothing
        push(Locations.at(7, 40.1900, 44.5133, T0 + 3000));     // outside -> EXIT

        assertThat(events()).extracting(GeofenceEvent::getTransition)
                .containsExactly(GeofenceTransition.ENTER, GeofenceTransition.EXIT);
        assertThat(events().get(0).getGeofenceName()).isEqualTo("Republic Square");
        assertThat(events().get(0).getDriverId()).isEqualTo(7L);
        assertThat(events().get(0).getEventId()).isNotBlank();
    }

    @Test
    void transitionsAreTrackedPerDriver() throws Exception {
        push(Locations.at(1, 40.1776, 44.5133, T0));            // driver 1 ENTER
        push(Locations.at(2, 40.1900, 44.5133, T0 + 1000));     // driver 2 outside -> nothing

        assertThat(events()).singleElement()
                .satisfies(e -> assertThat(e.getDriverId()).isEqualTo(1L));
    }
}

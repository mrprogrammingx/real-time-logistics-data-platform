package com.flowfleet.flink.driverstate;

import static org.assertj.core.api.Assertions.assertThat;

import com.flowfleet.events.avro.DriverLocation;
import com.flowfleet.events.avro.DriverSnapshot;
import com.flowfleet.flink.Locations;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DriverStateFunctionTest {

    private static final long T0 = 1_800_000_000_000L;

    private KeyedOneInputStreamOperatorTestHarness<Long, DriverLocation, DriverSnapshot> harness;

    @BeforeEach
    void setUp() throws Exception {
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new DriverStateFunction()),
                DriverLocation::getDriverId, Types.LONG);
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    private void push(DriverLocation loc) throws Exception {
        harness.processElement(loc, loc.getEventTime().toEpochMilli());
    }

    private List<DriverSnapshot> snapshots() {
        return harness.extractOutputValues();
    }

    @Test
    void firstSampleHasNoDerivedSpeed() throws Exception {
        push(Locations.at(7, 40.1776, 44.5126, T0));

        assertThat(snapshots()).singleElement().satisfies(s -> {
            assertThat(s.getDriverId()).isEqualTo(7L);
            assertThat(s.getSampleCount()).isEqualTo(1L);
            assertThat(s.getDerivedSpeedKph()).isNull();
            assertThat(s.getTripDistanceMeters()).isZero();
            assertThat(s.getPrevLatitude()).isNull();
        });
    }

    @Test
    void secondSampleDerivesSpeedAndDistanceFromTheDelta() throws Exception {
        push(Locations.at(7, 40.1776, 44.5126, T0));               // Republic Square
        push(Locations.at(7, 40.1872, 44.5153, T0 + 60_000));      // Opera, ~1.09 km, 60 s later

        DriverSnapshot second = snapshots().get(1);
        assertThat(second.getSampleCount()).isEqualTo(2L);
        assertThat(second.getDerivedSpeedKph()).isNotNull().isBetween(50.0, 80.0); // ~65 km/h
        assertThat(second.getTripDistanceMeters()).isBetween(900.0, 1300.0);
        assertThat(second.getPrevLatitude()).isEqualTo(40.1776);
    }

    @Test
    void keyedStateIsPerDriver() throws Exception {
        push(Locations.at(1, 40.10, 44.50, T0));
        push(Locations.at(2, 40.20, 44.55, T0));
        push(Locations.at(1, 40.11, 44.50, T0 + 1000));

        assertThat(snapshots()).filteredOn(s -> s.getDriverId() == 1L)
                .last().satisfies(s -> assertThat(s.getSampleCount()).isEqualTo(2L));
        assertThat(snapshots()).filteredOn(s -> s.getDriverId() == 2L)
                .allSatisfy(s -> assertThat(s.getSampleCount()).isEqualTo(1L));
    }

    @Test
    void gapTimerDropsStateWhenTheDriverGoesSilent() throws Exception {
        push(Locations.at(7, 40.1776, 44.5126, T0));
        // no sample for longer than GAP_MS of event time -> the timer fires, state is cleared
        harness.processWatermark(T0 + DriverStateFunction.GAP_MS + 1);
        push(Locations.at(7, 40.20, 44.55, T0 + DriverStateFunction.GAP_MS + 2));

        assertThat(snapshots()).last()
                .satisfies(s -> assertThat(s.getSampleCount()).isEqualTo(1L)); // fresh state
    }
}

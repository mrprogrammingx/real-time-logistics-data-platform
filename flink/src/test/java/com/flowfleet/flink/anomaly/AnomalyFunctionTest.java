package com.flowfleet.flink.anomaly;

import static org.assertj.core.api.Assertions.assertThat;

import com.flowfleet.events.avro.AlertKind;
import com.flowfleet.events.avro.DeliveryAlert;
import com.flowfleet.events.avro.DriverLocation;
import com.flowfleet.flink.Locations;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AnomalyFunctionTest {

    private static final long T0 = 1_800_000_000_000L;

    private KeyedOneInputStreamOperatorTestHarness<Long, DriverLocation, DeliveryAlert> harness;

    @BeforeEach
    void setUp() throws Exception {
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new AnomalyFunction()),
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

    @Test
    void normalMovementRaisesNoAlert() throws Exception {
        push(Locations.at(7, 40.1776, 44.5126, T0));
        push(Locations.at(7, 40.1786, 44.5126, T0 + 20_000));   // ~110 m in 20 s -> ~20 km/h

        assertThat(harness.extractOutputValues()).isEmpty();
    }

    @Test
    void impossibleSpeedBetweenConsecutiveSamplesRaisesAnAlert() throws Exception {
        push(Locations.at(7, 40.1776, 44.5126, T0));
        push(Locations.at(7, 40.3000, 44.7000, T0 + 5_000));    // ~24 km in 5 s

        assertThat(harness.extractOutputValues()).singleElement().satisfies(a -> {
            assertThat(a.getKind()).isEqualTo(AlertKind.IMPOSSIBLE_SPEED);
            assertThat(a.getDriverId()).isEqualTo(7L);
            assertThat(a.getDetail()).contains("km/h");
        });
    }

    @Test
    void gpsGapTimerRaisesAnAlert() throws Exception {
        push(Locations.at(7, 40.1776, 44.5126, T0));
        harness.processWatermark(T0 + AnomalyFunction.GAP_MS + 1);

        assertThat(harness.extractOutputValues()).singleElement().satisfies(a -> {
            assertThat(a.getKind()).isEqualTo(AlertKind.GPS_GAP);
            assertThat(a.getDriverId()).isEqualTo(7L);
        });
    }
}

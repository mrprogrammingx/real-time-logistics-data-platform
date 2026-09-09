package com.flowfleet.generator;

import static org.assertj.core.api.Assertions.assertThat;

import com.flowfleet.common.domain.GeoPoint;
import com.flowfleet.events.Topics;
import com.flowfleet.events.avro.DriverLocation;
import com.flowfleet.generator.config.GeneratorProperties;
import com.flowfleet.generator.sim.Fleet;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class FleetTest {

    private static GeneratorProperties props(int drivers, long seed) {
        return new GeneratorProperties(drivers, Duration.ofSeconds(1), 1.0, seed, false, false,
                Topics.DRIVER_LOCATIONS, "localhost:9092", "http://localhost:8085");
    }

    @Test
    void tickProducesOneSamplePerDriver() {
        Fleet fleet = new Fleet(props(50, 1L));
        List<DriverLocation> batch = fleet.tick(Duration.ofSeconds(1), Instant.now());

        assertThat(batch).hasSize(50);
        assertThat(batch).allSatisfy(e -> {
            assertThat(e.getDriverId()).isBetween(1L, 50L);
            assertThat(GeoPoint.isValid(e.getLatitude(), e.getLongitude())).isTrue();
            assertThat(e.getSpeedKph()).isNotNull().isPositive();
            assertThat(e.getHeadingDegrees()).isBetween(0, 359);
        });
        assertThat(batch.stream().map(DriverLocation::getDriverId).distinct().count()).isEqualTo(50);
    }

    @Test
    void sameSeedProducesSameMovement() {
        List<DriverLocation> a = new Fleet(props(10, 99L)).tick(Duration.ofSeconds(1), Instant.EPOCH);
        List<DriverLocation> b = new Fleet(props(10, 99L)).tick(Duration.ofSeconds(1), Instant.EPOCH);

        for (int i = 0; i < a.size(); i++) {
            assertThat(a.get(i).getLatitude()).isEqualTo(b.get(i).getLatitude());
            assertThat(a.get(i).getLongitude()).isEqualTo(b.get(i).getLongitude());
        }
    }

    @Test
    void driversActuallyMoveOverTime() {
        Fleet fleet = new Fleet(props(20, 5L));
        var first = fleet.tick(Duration.ofSeconds(1), Instant.EPOCH);
        List<DriverLocation> later = first;
        for (int i = 0; i < 30; i++) {
            later = fleet.tick(Duration.ofSeconds(1), Instant.EPOCH.plusSeconds(i + 1));
        }
        boolean anyMoved = false;
        for (int i = 0; i < first.size(); i++) {
            GeoPoint p0 = new GeoPoint(first.get(i).getLatitude(), first.get(i).getLongitude());
            GeoPoint p1 = new GeoPoint(later.get(i).getLatitude(), later.get(i).getLongitude());
            if (p0.distanceMetersTo(p1) > 50) {
                anyMoved = true;
            }
        }
        assertThat(anyMoved).isTrue();
    }
}

package com.flowfleet.flink.geo;

import static org.assertj.core.api.Assertions.assertThat;

import com.flowfleet.flink.geofence.GeofenceJob;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.prep.PreparedGeometry;

class GeometriesTest {

    // ~120 m box around Republic Square (lon lat pairs)
    private static final String REPUBLIC_SQ =
            "POLYGON((44.5126 40.1770, 44.5140 40.1770, 44.5140 40.1782, 44.5126 40.1782, 44.5126 40.1770))";

    @Test
    void pointInsideAndOutsideThePolygon() {
        PreparedGeometry fence = Geometries.prepareWkt(REPUBLIC_SQ);

        assertThat(Geometries.contains(fence, 40.1776, 44.5133)).isTrue();   // centre
        assertThat(Geometries.contains(fence, 40.1900, 44.5133)).isFalse();  // 1.3 km north
        assertThat(Geometries.contains(fence, 40.1776, 44.5300)).isFalse();  // east
    }

    @Test
    void badWktIsRejected() {
        assertThat(catchThrowable(() -> Geometries.prepareWkt("not wkt")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void theBundledGeofencesParse() throws Exception {
        List<Geofence> fences = GeofenceJob.loadGeofences();
        assertThat(fences).hasSize(4);
        fences.forEach(f -> assertThat(Geometries.prepareWkt(f.polygonWkt)).isNotNull());

        // the Kentron zone should contain all three restaurant centres
        PreparedGeometry zone = Geometries.prepareWkt(
                fences.stream().filter(f -> f.type.equals("ZONE")).findFirst().orElseThrow().polygonWkt);
        assertThat(Geometries.contains(zone, 40.1776, 44.5133)).isTrue();
        assertThat(Geometries.contains(zone, 40.1907, 44.5194)).isTrue();
    }

    private static Throwable catchThrowable(Runnable r) {
        try {
            r.run();
            return null;
        } catch (Throwable t) {
            return t;
        }
    }
}

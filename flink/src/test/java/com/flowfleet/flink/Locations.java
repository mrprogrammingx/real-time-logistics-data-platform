package com.flowfleet.flink;

import com.flowfleet.events.avro.DriverLocation;
import java.time.Instant;

/** Test factory for {@link DriverLocation} samples. */
public final class Locations {

    private Locations() {}

    public static DriverLocation at(long driverId, double lat, double lon, long eventTimeMs) {
        return DriverLocation.newBuilder()
                .setDriverId(driverId)
                .setLatitude(lat)
                .setLongitude(lon)
                .setEventTime(Instant.ofEpochMilli(eventTimeMs))
                .setSpeedKph(null)
                .setHeadingDegrees(null)
                .setVehicleType("CAR")
                .build();
    }

    public static DriverLocation at(long driverId, double lat, double lon, long eventTimeMs,
                                    double speedKph, int headingDegrees) {
        DriverLocation l = at(driverId, lat, lon, eventTimeMs);
        l.setSpeedKph(speedKph);
        l.setHeadingDegrees(headingDegrees);
        return l;
    }
}

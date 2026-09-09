package com.flowfleet.generator.sim;

import com.flowfleet.common.domain.GeoPoint;
import com.flowfleet.common.domain.VehicleType;
import com.flowfleet.events.avro.DriverLocation;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.random.RandomGenerator;

/**
 * One simulated driver. Drives from its current position to a restaurant, then to a
 * customer drop-off, then picks a new job — forever. Each {@link #step} advances it along
 * the route by {@code cruiseKph} (plus a little noise) and yields a fresh GPS sample.
 */
final class DriverSim {

    private final long driverId;
    private final VehicleType vehicle;
    private final double cruiseKph;
    private final RandomGenerator rng;

    private GeoPoint position;
    private double headingDegrees;
    private double lastSpeedKph;
    private final Deque<GeoPoint> route = new ArrayDeque<>();

    DriverSim(long driverId, VehicleType vehicle, GeoPoint start, RandomGenerator rng) {
        this.driverId = driverId;
        this.vehicle = vehicle;
        this.cruiseKph = cruiseKphFor(vehicle);
        this.position = start;
        this.rng = rng;
        planNewJob();
    }

    long driverId() {
        return driverId;
    }

    /** Advance {@code seconds} of simulated time and return the resulting sample. */
    DriverLocation step(double seconds, Instant eventTime) {
        double speedKph = cruiseKph * (0.75 + 0.5 * rng.nextDouble());   // 75%–125% of cruise
        double metres = speedKph * (1000.0 / 3600.0) * seconds;

        GeoPoint next = route.isEmpty() ? position : position.moveToward(route.peekFirst(), metres);
        if (!route.isEmpty() && next.equals(route.peekFirst())) {
            route.pollFirst();
            if (route.isEmpty()) {
                planNewJob();
            }
        }
        if (!next.equals(position)) {
            headingDegrees = position.bearingDegreesTo(next);
        }
        this.position = next;
        this.lastSpeedKph = speedKph;

        return DriverLocation.newBuilder()
                .setDriverId(driverId)
                .setLatitude(round(next.latitude(), 6))
                .setLongitude(round(next.longitude(), 6))
                .setEventTime(eventTime)
                .setSpeedKph(round(speedKph, 1))
                .setHeadingDegrees((int) Math.round(headingDegrees))
                .setVehicleType(vehicle.name())
                .build();
    }

    private void planNewJob() {
        GeoPoint restaurant = Waypoints.RESTAURANTS.get(rng.nextInt(Waypoints.RESTAURANTS.size()));
        GeoPoint customer = new GeoPoint(
                lerp(Waypoints.LAT_MIN, Waypoints.LAT_MAX, rng.nextDouble()),
                lerp(Waypoints.LON_MIN, Waypoints.LON_MAX, rng.nextDouble()));
        route.addLast(restaurant);
        route.addLast(customer);
    }

    private static double cruiseKphFor(VehicleType v) {
        return switch (v) {
            case BICYCLE -> 16;
            case SCOOTER -> 22;
            case MOTORCYCLE -> 38;
            case CAR -> 30;
            case VAN -> 26;
        };
    }

    private static double lerp(double a, double b, double t) {
        return a + (b - a) * t;
    }

    private static double round(double v, int places) {
        double f = Math.pow(10, places);
        return Math.round(v * f) / f;
    }
}

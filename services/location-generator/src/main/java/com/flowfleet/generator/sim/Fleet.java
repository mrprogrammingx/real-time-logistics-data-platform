package com.flowfleet.generator.sim;

import com.flowfleet.common.domain.GeoPoint;
import com.flowfleet.common.domain.VehicleType;
import com.flowfleet.events.avro.DriverLocation;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.random.RandomGenerator;
import org.springframework.stereotype.Component;

import com.flowfleet.generator.config.GeneratorProperties;

/**
 * The whole simulated fleet. {@link #tick} advances every driver by one step of simulated
 * time and returns their new positions. Deterministic for a given seed.
 */
@Component
public class Fleet {

    private final List<DriverSim> drivers;
    private final double speedupFactor;

    public Fleet(GeneratorProperties props) {
        this.speedupFactor = props.speedupFactor();
        // SplittableRandom lives in java.base, so it works on a jlink'd minimal JRE image
        // (the LXM algorithms are in the optional jdk.random module).
        RandomGenerator base = new SplittableRandom(props.seed());
        VehicleType[] vehicles = VehicleType.values();

        this.drivers = new ArrayList<>(props.drivers());
        for (int i = 1; i <= props.drivers(); i++) {
            RandomGenerator perDriver = new SplittableRandom(props.seed() * 1_000_003L + i);
            VehicleType vehicle = vehicles[base.nextInt(vehicles.length)];
            GeoPoint start = new GeoPoint(
                    Waypoints.LAT_MIN + (Waypoints.LAT_MAX - Waypoints.LAT_MIN) * base.nextDouble(),
                    Waypoints.LON_MIN + (Waypoints.LON_MAX - Waypoints.LON_MIN) * base.nextDouble());
            drivers.add(new DriverSim(i, vehicle, start, perDriver));
        }
    }

    public int size() {
        return drivers.size();
    }

    /** One tick: {@code wallInterval * speedupFactor} of simulated movement for every driver. */
    public List<DriverLocation> tick(java.time.Duration wallInterval, Instant now) {
        return tickWindow(0, drivers.size(), wallInterval, now);
    }

    /**
     * Step {@code count} drivers starting at index {@code offset} (wrapping), and return
     * their samples. Used by the load-test rate mode, which advances a rotating window
     * over the fleet each tick instead of the whole fleet. {@code perDriverInterval} is how
     * much wall time each stepped driver represents (tick × fleet ÷ window).
     */
    public List<DriverLocation> tickWindow(int offset, int count,
            java.time.Duration perDriverInterval, Instant now) {
        int size = drivers.size();
        int n = Math.max(0, Math.min(count, size));
        double simSeconds = perDriverInterval.toMillis() / 1000.0 * speedupFactor;
        List<DriverLocation> batch = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            batch.add(drivers.get(Math.floorMod(offset + i, size)).step(simSeconds, now));
        }
        return batch;
    }
}

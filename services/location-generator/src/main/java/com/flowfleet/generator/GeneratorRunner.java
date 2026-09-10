package com.flowfleet.generator;

import com.flowfleet.events.TopicAdmin;
import com.flowfleet.events.Topics;
import com.flowfleet.events.avro.DriverLocation;
import com.flowfleet.generator.config.GeneratorProperties;
import com.flowfleet.generator.kafka.LocationProducer;
import com.flowfleet.generator.sim.Fleet;
import com.flowfleet.generator.sim.RatePlan;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Drives the simulation. Two modes:
 *
 * <ul>
 *   <li><b>legacy</b> ({@code target-rate = 0}): every {@code tickInterval} advance the
 *       whole {@link Fleet} and produce one sample per driver. Throughput = drivers ÷ tick.</li>
 *   <li><b>rate</b> ({@code target-rate > 0}): a fixed 100 ms tick; each tick emit exactly
 *       enough samples to hold the target events/second, advancing a rotating window over
 *       the fleet. The rate can be changed at runtime ({@code POST /api/load/rate}).</li>
 * </ul>
 */
@Component
public class GeneratorRunner implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(GeneratorRunner.class);
    private static final long RATE_TICK_MS = 100;

    private final GeneratorProperties props;
    private final Fleet fleet;
    private final LocationProducer producer;

    private final java.util.concurrent.atomic.AtomicReference<Double> targetRate =
            new java.util.concurrent.atomic.AtomicReference<>(0.0);
    private final RatePlan ratePlan = new RatePlan();
    private final AtomicLong windowCursor = new AtomicLong();

    private ScheduledExecutorService scheduler;
    private volatile boolean running = false;
    private long lastReportAt = 0;
    private long lastReportSent = 0;

    public GeneratorRunner(GeneratorProperties props, Fleet fleet, LocationProducer producer,
            MeterRegistry registry) {
        this.props = props;
        this.fleet = fleet;
        this.producer = producer;
        this.targetRate.set(props.targetRate());
        registry.gauge("flowfleet.generator.target.rate", targetRate, ref -> ref.get());
    }

    @Override
    public void start() {
        if (!props.enabled()) {
            log.info("generator disabled (flowfleet.generator.enabled=false)");
            return;
        }
        if (props.autoCreateTopics()) {
            TopicAdmin.createIfAbsent(props.bootstrapServers(),
                    Topics.local(props.locationsPartitions()));
        }
        boolean rateMode = targetRate.get() > 0;
        long periodMs = rateMode ? RATE_TICK_MS : props.tickInterval().toMillis();
        log.info("starting fleet: {} drivers, mode={} ({}), -> topic {}",
                fleet.size(), rateMode ? "rate" : "legacy",
                rateMode ? targetRate.get() + " ev/s" : "tick " + props.tickInterval(), props.topic());

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "generator-tick");
            t.setDaemon(true);
            return t;
        });
        Runnable tick = rateMode ? this::rateTick : this::legacyTick;
        scheduler.scheduleAtFixedRate(tick, periodMs, periodMs, TimeUnit.MILLISECONDS);
        running = true;
    }

    private void legacyTick() {
        try {
            fleet.tick(props.tickInterval(), Instant.now()).forEach(producer::send);
            report();
        } catch (Exception e) {
            log.error("tick failed", e);
        }
    }

    private void rateTick() {
        try {
            int n = ratePlan.eventsThisTick(targetRate.get(), RATE_TICK_MS);
            if (n > 0) {
                int window = Math.min(n, fleet.size());
                // wall time each stepped driver represents: tick × fleet ÷ window
                Duration perDriver = Duration.ofMillis(Math.max(1, RATE_TICK_MS * fleet.size() / window));
                int offset = (int) (windowCursor.getAndAdd(window) % fleet.size());
                List<DriverLocation> batch = fleet.tickWindow(offset, window, perDriver, Instant.now());
                batch.forEach(producer::send);
                if (n > fleet.size()) {
                    log.warn("target rate needs {} events/tick but fleet is only {}; capped. "
                            + "Raise GENERATOR_DRIVERS.", n, fleet.size());
                }
            }
            report();
        } catch (Exception e) {
            log.error("rate tick failed", e);
        }
    }

    /** Load-test control: change the target throughput without a restart. 0 pauses production. */
    public void setTargetRate(double eventsPerSecond) {
        double v = Math.max(0.0, eventsPerSecond);
        targetRate.set(v);
        log.info("target rate set to {} ev/s", v);
    }

    public double targetRate() {
        return targetRate.get();
    }

    public long sentTotal() {
        return producer.sentCount();
    }

    private void report() {
        long now = System.currentTimeMillis();
        if (now - lastReportAt < 1000) {
            return;
        }
        long total = producer.sentCount();
        long delta = total - lastReportSent;
        double seconds = lastReportAt == 0 ? 1.0 : (now - lastReportAt) / 1000.0;
        log.info("produced {} events ({}/s), {} total", delta, Math.round(delta / seconds), total);
        lastReportAt = now;
        lastReportSent = total;
    }

    @Override
    public void stop() {
        running = false;
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}

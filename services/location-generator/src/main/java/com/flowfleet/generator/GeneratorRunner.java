package com.flowfleet.generator;

import com.flowfleet.events.TopicAdmin;
import com.flowfleet.events.Topics;
import com.flowfleet.events.avro.DriverLocation;
import com.flowfleet.generator.config.GeneratorProperties;
import com.flowfleet.generator.kafka.LocationProducer;
import com.flowfleet.generator.sim.Fleet;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Drives the simulation: on a fixed wall-clock interval it advances the {@link Fleet} and
 * produces every driver's new position. Reports throughput once a second.
 */
@Component
public class GeneratorRunner implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(GeneratorRunner.class);

    private final GeneratorProperties props;
    private final Fleet fleet;
    private final LocationProducer producer;

    private ScheduledExecutorService scheduler;
    private volatile boolean running = false;
    private long lastReportAt = 0;
    private long lastReportSent = 0;

    public GeneratorRunner(GeneratorProperties props, Fleet fleet, LocationProducer producer) {
        this.props = props;
        this.fleet = fleet;
        this.producer = producer;
    }

    @Override
    public void start() {
        if (!props.enabled()) {
            log.info("generator disabled (flowfleet.generator.enabled=false)");
            return;
        }
        if (props.autoCreateTopics()) {
            TopicAdmin.createIfAbsent(props.bootstrapServers(), Topics.local());
        }
        log.info("starting fleet: {} drivers, tick={}, speedup={}x -> topic {}",
                fleet.size(), props.tickInterval(), props.speedupFactor(), props.topic());

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "generator-tick");
            t.setDaemon(true);
            return t;
        });
        long periodMs = props.tickInterval().toMillis();
        scheduler.scheduleAtFixedRate(this::tick, periodMs, periodMs, TimeUnit.MILLISECONDS);
        running = true;
    }

    private void tick() {
        try {
            List<DriverLocation> batch = fleet.tick(props.tickInterval(), Instant.now());
            batch.forEach(producer::send);
            report();
        } catch (Exception e) {
            log.error("tick failed", e);
        }
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

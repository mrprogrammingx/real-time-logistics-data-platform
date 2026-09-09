package com.flowfleet.generator.config;

import com.flowfleet.events.Topics;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param drivers          number of simulated drivers
 * @param tickInterval     wall-clock interval between simulation ticks
 * @param speedupFactor    simulated time advances this many times faster than wall time
 * @param seed             RNG seed, so a run is reproducible
 * @param enabled          set false to load the context without starting the fleet (tests)
 * @param autoCreateTopics create the FlowFleet topics on startup if absent
 * @param topic            destination topic
 * @param bootstrapServers Kafka bootstrap
 * @param schemaRegistryUrl Confluent Schema Registry URL
 */
@ConfigurationProperties(prefix = "flowfleet.generator")
public record GeneratorProperties(
        int drivers,
        Duration tickInterval,
        double speedupFactor,
        long seed,
        boolean enabled,
        boolean autoCreateTopics,
        String topic,
        String bootstrapServers,
        String schemaRegistryUrl) {

    public GeneratorProperties {
        if (drivers <= 0) {
            drivers = 200;
        }
        if (tickInterval == null || tickInterval.isZero() || tickInterval.isNegative()) {
            tickInterval = Duration.ofSeconds(1);
        }
        if (speedupFactor <= 0) {
            speedupFactor = 1.0;
        }
        if (topic == null || topic.isBlank()) {
            topic = Topics.DRIVER_LOCATIONS;
        }
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            bootstrapServers = "localhost:19092";
        }
        if (schemaRegistryUrl == null || schemaRegistryUrl.isBlank()) {
            schemaRegistryUrl = "http://localhost:18085";
        }
    }
}

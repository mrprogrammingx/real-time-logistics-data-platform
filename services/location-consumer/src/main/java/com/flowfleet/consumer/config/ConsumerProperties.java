package com.flowfleet.consumer.config;

import com.flowfleet.events.Topics;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "flowfleet.consumer")
public record ConsumerProperties(
        String groupId,
        String topic,
        String bootstrapServers,
        String schemaRegistryUrl,
        boolean enabled,
        String autoOffsetReset) {

    public ConsumerProperties {
        if (groupId == null || groupId.isBlank()) {
            groupId = Topics.LOCATION_CONSUMER_GROUP;
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
        if (autoOffsetReset == null || autoOffsetReset.isBlank()) {
            autoOffsetReset = "earliest";
        }
    }
}

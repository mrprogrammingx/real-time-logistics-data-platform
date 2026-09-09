package com.flowfleet.events;

import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;

/**
 * Declarative description of a Kafka topic: name, partition count, replication factor and
 * a handful of topic-level configs. See {@code architecture/kafka-design.md} for the
 * reasoning behind each number.
 */
public record TopicSpec(
        String name,
        int partitions,
        short replicationFactor,
        Map<String, String> configs) {

    public TopicSpec(String name, int partitions, short replicationFactor) {
        this(name, partitions, replicationFactor, Map.of());
    }

    /** Retention as a topic config map, e.g. {@code retentionMs(Duration.ofHours(24))}. */
    public static Map<String, String> retention(java.time.Duration d) {
        return Map.of("retention.ms", Long.toString(d.toMillis()));
    }

    public NewTopic toNewTopic() {
        NewTopic t = new NewTopic(name, partitions, replicationFactor);
        if (!configs.isEmpty()) {
            t.configs(configs);
        }
        return t;
    }
}

package com.flowfleet.generator;

import org.testcontainers.redpanda.RedpandaContainer;

/**
 * Base for Kafka integration tests. Redpanda gives us a Kafka API <em>and</em> a
 * Confluent-compatible Schema Registry in a single container — enough to exercise the full
 * Avro serialize → register → produce → fetch → deserialize path without standing up
 * cp-kafka + cp-schema-registry. The production compose stack still uses the real ones.
 *
 * <p>Singleton container (same rationale as the API's {@code AbstractPostgisIT}).
 */
public abstract class AbstractRedpandaIT {

    protected static final RedpandaContainer REDPANDA =
            new RedpandaContainer("redpandadata/redpanda:v24.2.7");

    static {
        REDPANDA.start();
    }

    protected static String bootstrapServers() {
        return REDPANDA.getBootstrapServers();
    }

    protected static String schemaRegistryUrl() {
        return REDPANDA.getSchemaRegistryAddress();
    }
}

package com.flowfleet.generator.kafka;

import com.flowfleet.events.avro.DriverLocation;
import com.flowfleet.generator.config.GeneratorProperties;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import jakarta.annotation.PreDestroy;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Thin wrapper over a {@link KafkaProducer} configured for Avro + Schema Registry.
 *
 * <p>Config choices worth defending: {@code acks=all} + {@code enable.idempotence=true}
 * (no duplicates from producer retries, no silent loss), {@code compression.type=zstd},
 * {@code linger.ms=20} to batch under load. Keyed by {@code driverId} so all of one
 * driver's samples land on one partition, in order.
 */
@Component
public class LocationProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LocationProducer.class);

    private final Producer<String, DriverLocation> producer;
    private final String topic;
    private final KafkaClientMetrics metrics;
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();

    public LocationProducer(GeneratorProperties props, MeterRegistry registry) {
        this.topic = props.topic();
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.bootstrapServers());
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "flowfleet-location-generator");
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        p.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, props.schemaRegistryUrl());
        p.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        p.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        p.put(ProducerConfig.BATCH_SIZE_CONFIG, 64 * 1024);
        this.producer = new KafkaProducer<>(p);

        this.metrics = new KafkaClientMetrics(producer);
        this.metrics.bindTo(registry);
        // Monotonic counters -> Prometheus `flowfleet_generator_sent_total` etc., so the
        // load test can `rate()` them.
        FunctionCounter.builder("flowfleet.generator.sent", sent, AtomicLong::doubleValue)
                .description("DriverLocation events acked by Kafka").register(registry);
        FunctionCounter.builder("flowfleet.generator.failed", failed, AtomicLong::doubleValue)
                .description("DriverLocation events that failed to produce").register(registry);
    }

    public void send(DriverLocation event) {
        var record = new ProducerRecord<String, DriverLocation>(
                topic, String.valueOf(event.getDriverId()), event);
        producer.send(record, (md, ex) -> {
            if (ex != null) {
                failed.incrementAndGet();
                log.warn("produce failed: {}", ex.toString());
            } else {
                sent.incrementAndGet();
            }
        });
    }

    public long sentCount() {
        return sent.get();
    }

    @PreDestroy
    @Override
    public void close() {
        try {
            producer.flush();
        } finally {
            metrics.close();
            producer.close();
            log.info("producer closed; sent={} failed={}", sent.get(), failed.get());
        }
    }
}

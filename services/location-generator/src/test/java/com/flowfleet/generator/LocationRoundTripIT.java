package com.flowfleet.generator;

import static org.assertj.core.api.Assertions.assertThat;

import com.flowfleet.events.TopicAdmin;
import com.flowfleet.events.Topics;
import com.flowfleet.events.avro.DriverLocation;
import com.flowfleet.generator.config.GeneratorProperties;
import com.flowfleet.generator.kafka.LocationProducer;
import com.flowfleet.generator.sim.Fleet;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

class LocationRoundTripIT extends AbstractRedpandaIT {

    private static GeneratorProperties props(int drivers) {
        return new GeneratorProperties(
                drivers, Duration.ofSeconds(1), 1.0, 7L, true, true,
                Topics.DRIVER_LOCATIONS, bootstrapServers(), schemaRegistryUrl());
    }

    @Test
    void avroEventsSurviveKafkaAndSchemaRegistryRoundTrip() {
        var registry = new SimpleMeterRegistry();
        TopicAdmin.createIfAbsent(bootstrapServers(), Topics.local());

        Fleet fleet = new Fleet(props(5));
        try (LocationProducer producer = new LocationProducer(props(5), registry)) {
            List<DriverLocation> batch = fleet.tick(Duration.ofSeconds(1), Instant.now());
            batch.forEach(producer::send);
        }

        try (KafkaConsumer<String, DriverLocation> consumer = new KafkaConsumer<>(consumerProps())) {
            consumer.subscribe(List.of(Topics.DRIVER_LOCATIONS));

            Set<Long> driverIds = new HashSet<>();
            Map<Long, DriverLocation> lastByDriver = new HashMap<>();
            long deadline = System.currentTimeMillis() + 20_000;
            while (driverIds.size() < 5 && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(r -> {
                    driverIds.add(r.value().getDriverId());
                    lastByDriver.put(r.value().getDriverId(), r.value());
                    assertThat(r.key()).isEqualTo(String.valueOf(r.value().getDriverId()));
                });
            }

            assertThat(driverIds).containsExactlyInAnyOrder(1L, 2L, 3L, 4L, 5L);
            DriverLocation sample = lastByDriver.values().iterator().next();
            assertThat(sample.getSpeedKph()).isNotNull();          // v2 field populated
            assertThat(sample.getHeadingDegrees()).isBetween(0, 359);
            assertThat(sample.getVehicleType()).isNotBlank();
            assertThat(sample.getEventTime()).isInstanceOf(Instant.class);
        }
    }

    private static Properties consumerProps() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "round-trip-it");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        p.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl());
        p.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return p;
    }
}

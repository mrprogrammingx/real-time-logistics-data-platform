package com.flowfleet.flink;

import com.flowfleet.events.Topics;
import com.flowfleet.flink.ingest.ParsedLocation;
import com.flowfleet.flink.ingest.ResilientLocationDeserializer;
import java.nio.charset.StandardCharsets;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.util.function.SerializableFunction;

/**
 * {@link KafkaSource} / {@link KafkaSink} builders for FlowFleet's Confluent-Avro topics.
 * Sources start at {@code earliest} (dev), sinks are keyed and at-least-once (idempotent
 * sinks downstream, see {@code architecture/idempotency.md}).
 */
public final class KafkaIO {

    private KafkaIO() {}

    public static <T extends SpecificRecord> KafkaSource<T> avroSource(
            JobConfig cfg, String topic, String groupId, Class<T> type) {
        return KafkaSource.<T>builder()
                .setBootstrapServers(cfg.bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(
                        ConfluentRegistryAvroDeserializationSchema.forSpecific(type, cfg.schemaRegistryUrl))
                .build();
    }

    /**
     * {@code flowfleet.driver.locations} decoded resiliently — a poison message becomes a
     * {@link ParsedLocation} with {@code error} set instead of failing the job.
     * {@code LocationIngest} routes the failures to the DLQ.
     */
    public static KafkaSource<ParsedLocation> resilientLocationSource(JobConfig cfg, String groupId) {
        return KafkaSource.<ParsedLocation>builder()
                .setBootstrapServers(cfg.bootstrapServers)
                .setTopics(Topics.DRIVER_LOCATIONS)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new ResilientLocationDeserializer(cfg.schemaRegistryUrl))
                .build();
    }

    public static <T extends SpecificRecord> KafkaSink<T> avroSink(
            JobConfig cfg, String topic, Class<T> type, SerializableFunction<T, String> key) {
        return KafkaSink.<T>builder()
                .setBootstrapServers(cfg.bootstrapServers)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(KafkaRecordSerializationSchema.<T>builder()
                        .setTopic(topic)
                        .setKeySerializationSchema(new KeyString<>(key))
                        .setValueSerializationSchema(ConfluentRegistryAvroSerializationSchema.forSpecific(
                                type, topic + "-value", cfg.schemaRegistryUrl))
                        .build())
                .build();
    }

    /** Plain-string sink, e.g. for the DLQ topic. */
    public static KafkaSink<String> stringSink(JobConfig cfg, String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(cfg.bootstrapServers)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
    }

    /** Serializes a derived string key to UTF-8 bytes. */
    private record KeyString<T>(SerializableFunction<T, String> fn) implements SerializationSchema<T> {
        @Override
        public byte[] serialize(T element) {
            return fn.apply(element).getBytes(StandardCharsets.UTF_8);
        }
    }
}

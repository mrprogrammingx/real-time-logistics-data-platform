package com.flowfleet.flink.ingest;

import com.flowfleet.events.avro.DriverLocation;
import java.util.Base64;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * A Kafka deserializer that <strong>cannot fail the job</strong>: a record whose value
 * isn't decodable Confluent-Avro becomes a {@link ParsedLocation} with {@code error} set
 * (and its bytes + offset captured) instead of throwing. {@code LocationIngest} then routes
 * those to the DLQ.
 */
public class ResilientLocationDeserializer
        implements KafkaRecordDeserializationSchema<ParsedLocation> {

    private final String schemaRegistryUrl;
    private transient DeserializationSchema<DriverLocation> avro;

    public ResilientLocationDeserializer(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        avro = ConfluentRegistryAvroDeserializationSchema.forSpecific(
                DriverLocation.class, schemaRegistryUrl);
        avro.open(context);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<ParsedLocation> out) {
        ParsedLocation p = new ParsedLocation();
        p.topic = record.topic();
        p.partition = record.partition();
        p.offset = record.offset();
        p.timestamp = record.timestamp();
        try {
            p.value = avro.deserialize(record.value());
            if (p.value == null) {
                p.error = "tombstone / null value";
            }
        } catch (Exception e) {
            p.error = e.getClass().getSimpleName() + ": " + e.getMessage();
            byte[] raw = record.value();
            p.rawBase64 = raw == null ? null
                    : Base64.getEncoder().encodeToString(raw.length > 256
                        ? java.util.Arrays.copyOf(raw, 256) : raw);
        }
        out.collect(p);
    }

    @Override
    public TypeInformation<ParsedLocation> getProducedType() {
        return TypeInformation.of(ParsedLocation.class);
    }
}

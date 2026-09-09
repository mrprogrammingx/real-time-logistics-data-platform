package com.flowfleet.consumer.kafka;

import com.flowfleet.consumer.config.ConsumerProperties;
import com.flowfleet.consumer.stats.DriverSpeedStats;
import com.flowfleet.events.avro.DriverLocation;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * A hand-rolled poll loop (not Spring Kafka) so the consumer-group mechanics are explicit:
 *
 * <ul>
 *   <li>{@code enable.auto.commit=false} + {@link KafkaConsumer#commitSync()} after each
 *       processed batch — at-least-once, offsets committed only for work that's done;</li>
 *   <li>{@code CooperativeStickyAssignor} — a rebalance only moves the partitions that
 *       actually need to move, instead of stop-the-world;</li>
 *   <li>a {@link ConsumerRebalanceListener} that logs exactly which partitions this
 *       instance gains and loses — that log line is the Phase 2 experiment's evidence.</li>
 * </ul>
 */
@Component
public class LocationConsumerLoop implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(LocationConsumerLoop.class);

    private final ConsumerProperties props;
    private final DriverSpeedStats stats;
    private final MeterRegistry registry;

    private volatile boolean running = false;
    private KafkaConsumer<String, DriverLocation> consumer;
    private Thread thread;
    private final AtomicLong polled = new AtomicLong();

    public LocationConsumerLoop(ConsumerProperties props, DriverSpeedStats stats, MeterRegistry registry) {
        this.props = props;
        this.stats = stats;
        this.registry = registry;
    }

    @Override
    public void start() {
        if (!props.enabled()) {
            log.info("consumer disabled (flowfleet.consumer.enabled=false)");
            return;
        }
        registry.gauge("flowfleet.consumer.processed", stats, DriverSpeedStats::processed);
        registry.gauge("flowfleet.consumer.drivers_seen", stats, s -> s.driversSeen());

        consumer = new KafkaConsumer<>(consumerConfig());
        thread = new Thread(this::runLoop, "location-consumer");
        thread.setDaemon(true);
        running = true;
        thread.start();
        log.info("consumer started: group={} topic={}", props.groupId(), props.topic());
    }

    private Properties consumerConfig() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.bootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, props.groupId());
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, "flowfleet-location-consumer-" + shortId());
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        p.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, props.schemaRegistryUrl());
        p.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, props.autoOffsetReset());
        p.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        return p;
    }

    private void runLoop() {
        try {
            consumer.subscribe(List.of(props.topic()), new LoggingRebalanceListener());
            long lastReport = System.currentTimeMillis();
            long lastCount = 0;

            while (running) {
                ConsumerRecords<String, DriverLocation> records = consumer.poll(Duration.ofMillis(500));
                for (var rec : records) {
                    DriverLocation loc = rec.value();
                    if (loc != null) {
                        stats.record(loc.getDriverId(), loc.getSpeedKph());
                    }
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                    polled.addAndGet(records.count());
                }

                long now = System.currentTimeMillis();
                if (now - lastReport >= 5000) {
                    long total = polled.get();
                    double rate = (total - lastCount) / ((now - lastReport) / 1000.0);
                    log.info("consumed {} events ({}/s), lag={}, partitions={}",
                            total, Math.round(rate), totalLag(), assignmentString());
                    lastReport = now;
                    lastCount = total;
                }
            }
        } catch (WakeupException expectedOnShutdown) {
            // fall through
        } catch (Exception e) {
            log.error("consumer loop crashed", e);
        } finally {
            try {
                consumer.commitSync(Duration.ofSeconds(3));
            } catch (Exception ignored) {
                // best effort
            }
            consumer.close();
            log.info("consumer closed; processed {} events", stats.processed());
        }
    }

    private long totalLag() {
        try {
            return consumer.assignment().stream()
                    .mapToLong(tp -> consumer.currentLag(tp).orElse(0))
                    .sum();
        } catch (Exception e) {
            return -1;
        }
    }

    private String assignmentString() {
        return consumer.assignment().stream()
                .map(tp -> String.valueOf(tp.partition()))
                .sorted()
                .collect(Collectors.joining(",", "[", "]"));
    }

    private static String shortId() {
        return Long.toHexString(ProcessHandle.current().pid());
    }

    @Override
    public void stop() {
        running = false;
        if (consumer != null) {
            consumer.wakeup();
        }
        if (thread != null) {
            try {
                thread.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private final class LoggingRebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> revoked) {
            if (!revoked.isEmpty()) {
                consumer.commitSync();   // commit before we lose these partitions
                log.info("REBALANCE - revoked {}", partitions(revoked));
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> assigned) {
            log.info("REBALANCE + assigned {} (now own {})", partitions(assigned), assignmentString());
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> lost) {
            log.warn("REBALANCE ! lost {} (no commit possible)", partitions(lost));
        }

        private String partitions(Collection<TopicPartition> tps) {
            return tps.stream().map(tp -> tp.topic() + "-" + tp.partition())
                    .sorted().collect(Collectors.joining(","));
        }
    }
}

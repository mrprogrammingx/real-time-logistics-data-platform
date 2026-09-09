package com.flowfleet.cdc;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the Debezium PostgreSQL connector <em>in this process</em> via the embedded engine,
 * delivering parsed {@link CdcRecord}s to a callback.
 *
 * <p>This is the lightweight representation of the CDC pipeline — no Kafka Connect cluster.
 * The production path (see {@code kafka-connect/postgres-source.json} and
 * {@code docker-compose.yml}) runs the identical connector inside distributed Kafka
 * Connect; the moving parts it exercises are the same: logical replication slot, publication,
 * initial snapshot then WAL streaming, {@code op} c/r/u/d, before/after images, tombstones.
 */
public final class DebeziumCdcSource implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DebeziumCdcSource.class);

    private final DebeziumEngine<ChangeEvent<String, String>> engine;
    private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "debezium-engine");
        t.setDaemon(true);
        return t;
    });
    private final CountDownLatch stopped = new CountDownLatch(1);

    public DebeziumCdcSource(Properties config, Consumer<CdcRecord> onRecord) {
        CdcEnvelopeParser parser = new CdcEnvelopeParser();
        this.engine = DebeziumEngine.create(Json.class)
                .using(config)
                .using((success, message, error) -> {
                    if (error != null) {
                        log.error("Debezium engine stopped: {}", message, error);
                    } else {
                        log.info("Debezium engine stopped: {}", message);
                    }
                    stopped.countDown();
                })
                .notifying(event -> onRecord.accept(parser.parse(event.key(), event.value())))
                .build();
    }

    public void start() {
        executor.execute(engine);
    }

    /** Blocks until the engine has fully stopped (after {@link #close()}), or the timeout. */
    public boolean awaitStopped(Duration timeout) throws InterruptedException {
        return stopped.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws IOException {
        engine.close();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(15, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * A baseline embedded-engine configuration for PostgreSQL.
     *
     * @param slotName  logical replication slot; must be unique per running connector
     * @param tables    fully-qualified ({@code public.orders}) tables to capture
     * @param offsetFile where the engine persists its resume position between restarts
     * @param dropSlotOnStop test convenience — <strong>never</strong> true in production
     */
    public static Properties postgresConfig(
            String name, String host, int port, String db, String user, String password,
            String slotName, List<String> tables, Path offsetFile, boolean dropSlotOnStop) {

        Properties p = new Properties();
        p.setProperty("name", name);
        p.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");

        p.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        p.setProperty("offset.storage.file.filename", offsetFile.toString());
        p.setProperty("offset.flush.interval.ms", "1000");

        p.setProperty("topic.prefix", "flowfleet");
        p.setProperty("database.hostname", host);
        p.setProperty("database.port", Integer.toString(port));
        p.setProperty("database.user", user);
        p.setProperty("database.password", password);
        p.setProperty("database.dbname", db);

        p.setProperty("plugin.name", "pgoutput");
        p.setProperty("slot.name", slotName);
        p.setProperty("slot.drop.on.stop", Boolean.toString(dropSlotOnStop));
        p.setProperty("publication.name", "flowfleet_pub_" + slotName);
        p.setProperty("publication.autocreate.mode", "filtered");

        p.setProperty("table.include.list", String.join(",", tables));
        p.setProperty("snapshot.mode", "initial");
        p.setProperty("tombstones.on.delete", "true");
        p.setProperty("decimal.handling.mode", "string");
        p.setProperty("time.precision.mode", "connect");
        p.setProperty("topic.naming.strategy", "io.debezium.schema.DefaultTopicNamingStrategy");

        p.setProperty("key.converter.schemas.enable", "false");
        p.setProperty("value.converter.schemas.enable", "false");
        return p;
    }
}

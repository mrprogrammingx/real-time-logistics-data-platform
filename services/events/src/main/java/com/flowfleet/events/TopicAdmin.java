package com.flowfleet.events;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates the FlowFleet topics if they are absent. Idempotent: an already-existing topic
 * is left untouched (partition count and configs are <em>not</em> reconciled — changing
 * either is a deliberate, separate operation).
 */
public final class TopicAdmin {

    private static final Logger log = LoggerFactory.getLogger(TopicAdmin.class);

    private TopicAdmin() {}

    public static void createIfAbsent(String bootstrapServers, Collection<TopicSpec> specs) {
        try (Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            createIfAbsent(admin, specs);
        }
    }

    public static void createIfAbsent(Admin admin, Collection<TopicSpec> specs) {
        Set<String> existing;
        try {
            existing = admin.listTopics().names().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while listing topics", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("failed to list topics", e.getCause());
        }

        var toCreate = specs.stream()
                .filter(s -> !existing.contains(s.name()))
                .map(TopicSpec::toNewTopic)
                .toList();

        if (toCreate.isEmpty()) {
            log.info("all {} topics already present", specs.size());
            return;
        }

        try {
            admin.createTopics(toCreate).all().get();
            toCreate.forEach(t -> log.info("created topic {} ({} partitions)", t.name(), t.numPartitions()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while creating topics", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                log.info("topics were created concurrently by someone else — fine");
                return;
            }
            throw new IllegalStateException("failed to create topics", e.getCause());
        }
    }
}

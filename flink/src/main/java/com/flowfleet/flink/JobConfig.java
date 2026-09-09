package com.flowfleet.flink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Common job wiring: where Kafka and the Schema Registry live, and a
 * {@link StreamExecutionEnvironment} with checkpointing on.
 *
 * <p>Values come from {@code --key value} args first, then {@code KEY} env vars, then a
 * local default. Cluster-level tuning (unaligned/externalized checkpoints, restart backoff,
 * state backend, parallelism) belongs in {@code flink-conf.yaml} / the Phase 7
 * {@code FlinkDeployment} spec, not in code.
 */
public final class JobConfig {

    public final String bootstrapServers;
    public final String schemaRegistryUrl;
    public final String groupIdPrefix;

    private JobConfig(String bootstrapServers, String schemaRegistryUrl, String groupIdPrefix) {
        this.bootstrapServers = bootstrapServers;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.groupIdPrefix = groupIdPrefix;
    }

    public static JobConfig from(String[] args) {
        ParameterTool p = ParameterTool.fromArgs(args);
        return new JobConfig(
                pick(p, "bootstrap-servers", "KAFKA_BOOTSTRAP", "localhost:19092"),
                pick(p, "schema-registry-url", "SCHEMA_REGISTRY_URL", "http://localhost:18085"),
                pick(p, "group-id-prefix", "GROUP_ID_PREFIX", "flowfleet.flink"));
    }

    private static String pick(ParameterTool p, String arg, String env, String dflt) {
        if (p.has(arg)) {
            return p.get(arg);
        }
        String v = System.getenv(env);
        return v != null && !v.isBlank() ? v : dflt;
    }

    public String groupId(String jobName) {
        return groupIdPrefix + "." + jobName;
    }

    /** Checkpointing every 30s. */
    public static StreamExecutionEnvironment environment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000L);
        return env;
    }
}

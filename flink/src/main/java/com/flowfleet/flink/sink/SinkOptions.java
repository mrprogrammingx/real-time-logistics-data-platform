package com.flowfleet.flink.sink;

import org.apache.flink.api.java.utils.ParameterTool;

/** Where the analytical sinks write. Resolved from {@code --key value}, then env, then a local default. */
public final class SinkOptions {

    public final String timescaleUrl;
    public final String timescaleUser;
    public final String timescalePassword;

    public final String clickHouseUrl;
    public final String clickHouseUser;
    public final String clickHousePassword;

    /** Phase 6 backpressure experiment: sleep this many ms per record before the sink. 0 = off. */
    public final long slowMapMs;

    private SinkOptions(String tsUrl, String tsUser, String tsPw,
                        String chUrl, String chUser, String chPw, long slowMapMs) {
        this.timescaleUrl = tsUrl;
        this.timescaleUser = tsUser;
        this.timescalePassword = tsPw;
        this.clickHouseUrl = chUrl;
        this.clickHouseUser = chUser;
        this.clickHousePassword = chPw;
        this.slowMapMs = slowMapMs;
    }

    public static SinkOptions from(String[] args) {
        ParameterTool p = ParameterTool.fromArgs(args);
        return new SinkOptions(
                pick(p, "timescale-url", "TIMESCALE_URL", "jdbc:postgresql://localhost:15433/flowfleet"),
                pick(p, "timescale-user", "TIMESCALE_USER", "flowfleet"),
                pick(p, "timescale-password", "TIMESCALE_PASSWORD", "flowfleet"),
                pick(p, "clickhouse-url", "CLICKHOUSE_URL", "jdbc:clickhouse://localhost:18123/flowfleet"),
                pick(p, "clickhouse-user", "CLICKHOUSE_USER", "flowfleet"),
                pick(p, "clickhouse-password", "CLICKHOUSE_PASSWORD", "flowfleet"),
                Long.parseLong(pick(p, "slow-map-ms", "SLOW_MAP_MS", "0")));
    }

    public static SinkOptions of(String timescaleUrl, String timescaleUser, String timescalePassword,
                                 String clickHouseUrl, String clickHouseUser, String clickHousePassword) {
        return new SinkOptions(timescaleUrl, timescaleUser, timescalePassword,
                clickHouseUrl, clickHouseUser, clickHousePassword, 0L);
    }

    private static String pick(ParameterTool p, String arg, String env, String dflt) {
        if (p.has(arg)) {
            return p.get(arg);
        }
        String v = System.getenv(env);
        return v != null && !v.isBlank() ? v : dflt;
    }
}

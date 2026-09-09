package com.flowfleet.cdc;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * A parsed Debezium change event.
 *
 * <p>The Debezium envelope looks like:
 * <pre>
 * {
 *   "op": "u",                       c=create r=read(snapshot) u=update d=delete t=truncate
 *   "ts_ms": 1770000000000,
 *   "source": { "table": "orders", "lsn": 273827, "txId": 5581, "snapshot": "false", ... },
 *   "before": { ... },               present on u/d when REPLICA IDENTITY is FULL
 *   "after":  { ... }                present on c/r/u
 * }
 * </pre>
 *
 * <p>A delete is followed by a <em>tombstone</em>: a record whose value is {@code null}.
 * That is represented here as {@link #isTombstone()} with {@link #op()} == {@code DELETE}.
 */
public record CdcRecord(
        Op op,
        String table,
        long tsMs,
        Long lsn,
        boolean fromSnapshot,
        JsonNode before,
        JsonNode after,
        JsonNode key) {

    public enum Op {
        CREATE, READ, UPDATE, DELETE, TRUNCATE, MESSAGE, UNKNOWN;

        static Op fromCode(String code) {
            if (code == null) {
                return UNKNOWN;
            }
            return switch (code) {
                case "c" -> CREATE;
                case "r" -> READ;
                case "u" -> UPDATE;
                case "d" -> DELETE;
                case "t" -> TRUNCATE;
                case "m" -> MESSAGE;
                default -> UNKNOWN;
            };
        }
    }

    /** True for the null-value record Kafka emits after a delete on a compacted topic. */
    public boolean isTombstone() {
        return op == Op.DELETE && before == null && after == null;
    }

    /** The row image that survives the change (after for c/r/u, before for d). */
    public JsonNode current() {
        return op == Op.DELETE ? before : after;
    }
}

package com.flowfleet.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Turns the raw Debezium key/value JSON strings (schemas disabled) into a {@link CdcRecord}.
 * A {@code null} value string is a Kafka tombstone.
 */
public final class CdcEnvelopeParser {

    private final ObjectMapper mapper = new ObjectMapper();

    public CdcRecord parse(String keyJson, String valueJson) {
        try {
            JsonNode key = keyJson == null ? null : mapper.readTree(keyJson);

            if (valueJson == null) {
                // tombstone: value is null; the table comes from the key subject at the call site
                return new CdcRecord(CdcRecord.Op.DELETE, null, 0L, null, false, null, null, key);
            }

            JsonNode v = mapper.readTree(valueJson);
            JsonNode source = v.path("source");
            CdcRecord.Op op = CdcRecord.Op.fromCode(v.path("op").asText(null));
            String table = source.path("table").asText(null);
            long tsMs = v.path("ts_ms").asLong(0L);
            Long lsn = source.hasNonNull("lsn") ? source.path("lsn").asLong() : null;
            boolean snapshot = !"false".equals(source.path("snapshot").asText("false"));

            JsonNode before = nullIfMissing(v, "before");
            JsonNode after = nullIfMissing(v, "after");
            return new CdcRecord(op, table, tsMs, lsn, snapshot, before, after, key);
        } catch (Exception e) {
            throw new IllegalArgumentException("unparseable Debezium envelope: " + e.getMessage(), e);
        }
    }

    private static JsonNode nullIfMissing(JsonNode parent, String field) {
        JsonNode n = parent.get(field);
        return (n == null || n.isNull()) ? null : n;
    }
}

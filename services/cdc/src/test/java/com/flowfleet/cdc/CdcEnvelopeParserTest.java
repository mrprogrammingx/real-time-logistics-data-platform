package com.flowfleet.cdc;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class CdcEnvelopeParserTest {

    private final CdcEnvelopeParser parser = new CdcEnvelopeParser();

    @Test
    void parsesAnUpdateEnvelope() {
        String value = """
            {
              "op": "u",
              "ts_ms": 1770000000000,
              "source": { "table": "orders", "lsn": 273827, "txId": 5581, "snapshot": "false" },
              "before": { "id": 123, "status": "OUT_FOR_DELIVERY" },
              "after":  { "id": 123, "status": "DELIVERED" }
            }
            """;
        CdcRecord r = parser.parse("{\"id\":123}", value);

        assertThat(r.op()).isEqualTo(CdcRecord.Op.UPDATE);
        assertThat(r.table()).isEqualTo("orders");
        assertThat(r.lsn()).isEqualTo(273827L);
        assertThat(r.fromSnapshot()).isFalse();
        assertThat(r.before().get("status").asText()).isEqualTo("OUT_FOR_DELIVERY");
        assertThat(r.after().get("status").asText()).isEqualTo("DELIVERED");
        assertThat(r.current().get("status").asText()).isEqualTo("DELIVERED");
        assertThat(r.isTombstone()).isFalse();
    }

    @Test
    void parsesASnapshotRead() {
        String value = """
            { "op": "r", "ts_ms": 1, "source": { "table": "drivers", "snapshot": "true" },
              "after": { "id": 1, "name": "Karen" } }
            """;
        CdcRecord r = parser.parse("{\"id\":1}", value);

        assertThat(r.op()).isEqualTo(CdcRecord.Op.READ);
        assertThat(r.fromSnapshot()).isTrue();
        assertThat(r.before()).isNull();
    }

    @Test
    void parsesADeleteAndItsTombstone() {
        String del = """
            { "op": "d", "ts_ms": 2, "source": { "table": "orders", "snapshot": "false" },
              "before": { "id": 9 }, "after": null }
            """;
        CdcRecord d = parser.parse("{\"id\":9}", del);
        assertThat(d.op()).isEqualTo(CdcRecord.Op.DELETE);
        assertThat(d.after()).isNull();
        assertThat(d.before().get("id").asInt()).isEqualTo(9);
        assertThat(d.isTombstone()).isFalse();   // delete event still carries `before`

        CdcRecord tombstone = parser.parse("{\"id\":9}", null);
        assertThat(tombstone.isTombstone()).isTrue();
        assertThat(tombstone.op()).isEqualTo(CdcRecord.Op.DELETE);
    }
}

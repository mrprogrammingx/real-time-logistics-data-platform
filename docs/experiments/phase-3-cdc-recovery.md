# Experiment: CDC snapshot, streaming, and connector recovery

**Setup:** `make up && make cdc-register`. The API seeds a few rows; the generator does not
touch the captured tables.

## 1. Initial snapshot vs streaming

```bash
make cdc-tail        # flowfleet.orders.cdc from the beginning
```

Expect: a burst of `"op":"r"` (snapshot reads, `source.snapshot != "false"`) for rows that
existed at connector start, then — as you `make smoke` / `make cdc-demo` — `"op":"c"` /
`"op":"u"` with `source.snapshot":"false"` and a monotonically increasing `source.lsn`.

## 2. INSERT / UPDATE / DELETE + tombstone

```bash
make cdc-demo
docker compose exec postgres psql -U flowfleet -d flowfleet -c \
  "DELETE FROM orders WHERE id = (SELECT max(id) FROM orders)"
make cdc-tail
```

Expect the delete as `{op:d, before:{...full row...}, after:null}` (full `before` because
`REPLICA IDENTITY FULL`) immediately followed by a tombstone (key set, value null).

## 3. Connector restart → replay

```bash
./scripts/connect.sh offsets           # note the committed LSN
docker compose restart kafka-connect
./scripts/connect.sh status            # RUNNING again
```

Expect: it resumes from the committed offset; a small tail of already-seen changes may be
re-emitted (at-least-once). No `source.lsn` gap. Downstream idempotency (Phase 5) makes the
replay a no-op.

## 4. Replication-slot pressure

```bash
./scripts/connect.sh pause
# make some order changes via the API...
make cdc-slot         # active=f, `retained` grows — Postgres is holding WAL for the slot
./scripts/connect.sh resume
make cdc-slot         # retained drops back down as Debezium catches up
```

Talking point: a **dead** connector whose slot is never dropped will fill the disk. Monitor
`pg_replication_slots` lag; drop truly-abandoned slots.

## 5. Postgres restart

```bash
docker compose restart postgres
./scripts/connect.sh status     # reconnects, continues from confirmed_flush_lsn
```

## Results (fill in from your run)

```
date:
debezium: 3.0.8.Final   connect: cp-kafka-connect 7.8.0

snapshot: ___ rows as op=r
insert->op=c latency:  ___ ms
update carries before+after:  yes / no
delete + tombstone:  yes / no
restart replay:  ___ duplicate records, 0 gaps
slot retained while paused 60s: ___
```

# CDC design (Debezium → Kafka)

> Status: **implemented** (Phase 3).
> - Production path: `services/connect/Dockerfile` (Kafka Connect + Debezium PG connector),
>   `kafka-connect/postgres-source.json`, `scripts/connect.sh` / `make cdc-*`.
> - Test path: `services/cdc` runs the same connector via the Debezium **embedded engine**
>   against a PostgreSQL Testcontainer (`PostgresCdcIT`) — proves snapshot → streaming,
>   `op` c/r/u/d, before/after images and tombstones without a Connect cluster.
> - `V3__cdc_replica_identity.sql` sets `REPLICA IDENTITY FULL` on the captured tables.

## Why CDC instead of dual-writes

The API could publish to Kafka after each DB write, but then a crash between the commit and
the publish loses an event (or a publish without a commit invents one). CDC reads the
**write-ahead log** — the same thing that makes the commit durable — so the event stream
can't diverge from the database. One writer, one source of truth, one ordering.

## PostgreSQL setup

`docker-compose.yml` already starts Postgres with:

```
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
```

Debezium then needs:

* a **publication** (`FOR TABLE public.orders, public.drivers, public.deliveries, public.driver_shifts`)
* a **logical replication slot** (`pgoutput` plugin) — this is durable server-side state
  that holds WAL until Debezium confirms consumption. **A stopped connector with a live
  slot will fill the disk.** Monitored in Phase 7.
* `REPLICA IDENTITY FULL` on tables where we want full `before` images on update/delete
  (default `DEFAULT` only gives PK in `before`).

## Snapshot vs streaming

1. **Initial snapshot** — on first start the connector `SELECT`s every row of every
   captured table and emits it as a `read` (`op: "r"`) event, establishing a consistent
   baseline at a single LSN.
2. **Streaming** — from that LSN forward it tails the WAL, emitting `c` / `u` / `d`.
3. **Incremental snapshots** (signal-triggered) — add a table, or re-snapshot one, without
   stopping streaming; the connector interleaves snapshot chunks with live changes using
   watermarking (Debezium DBLog). Used when we add `customers` / `restaurants` later.

## Event shape

```json
{
  "op": "u",                         // c=create r=read(snapshot) u=update d=delete
  "ts_ms": 1770000000000,            // when the connector processed it
  "source": { "lsn": 273827, "txId": 5581, "ts_ms": 1769999999900, "table": "orders" },
  "before": { "id": 123, "status": "OUT_FOR_DELIVERY", ... },
  "after":  { "id": 123, "status": "DELIVERED", ... }
}
```

* **delete** → one event with `after: null`, immediately followed by a **tombstone**
  (`key`, `value: null`) so log-compacted topics can drop the key.
* Consumers must handle `before`/`after` both being partially populated depending on
  `REPLICA IDENTITY`.

## Topics & routing

Debezium's default topic is `<topic.prefix>.<schema>.<table>` = `flowfleet.public.orders`.
A `RegexRouter` SMT rewrites that to the catalogue name:

```
"transforms.route.regex":       "flowfleet\\.public\\.(.*)"
"transforms.route.replacement": "flowfleet.$1.cdc"        ->  flowfleet.orders.cdc
```

Value + key are **Avro** via the Confluent `AvroConverter`; Debezium registers the envelope
and key schemas in Schema Registry (subjects `flowfleet.orders.cdc-value` / `-key`).

## Ordering & keys

Debezium keys each record by the table PK ⇒ per-row total order on one partition (see
[`kafka-design.md`](kafka-design.md)). Cross-table ordering is **not** guaranteed; the
Flink jobs reconstruct causal order from `source.lsn` / `source.ts_ms` and event-time
watermarks, never from arrival order.

## Restart & recovery

| Failure | What happens | What we verify (Phase 3) |
|---------|--------------|--------------------------|
| Connector restarts | resumes from last committed **offset** (LSN) in `connect-offsets`; replays anything not yet acked ⇒ **duplicates possible** downstream | idempotent consumers absorb the replay |
| Kafka Connect worker dies | task rebalanced to another worker (distributed mode), resumes from same offset | no gap in the LSN sequence |
| PostgreSQL restarts | slot persists; connector reconnects and continues from slot's confirmed LSN | no lost changes |
| Connector down for hours | replication slot holds WAL ⇒ disk grows; on restart it catches up | alert on `pg_replication_slots.confirmed_flush_lsn` lag |
| Snapshot interrupted | non-incremental snapshot restarts from scratch; incremental resumes mid-chunk | prefer incremental for large tables |

## Schema evolution

* **Additive DB change** (new nullable column): Debezium emits an updated schema; Avro
  `BACKWARD` compatibility ⇒ old consumers ignore the new field, new consumers read it.
* **Rename / type change / drop**: breaking. Handled by expand-then-contract — add new
  column, dual-write in the app, migrate consumers, drop old column in a later release.
* Debezium's `schema history topic` lets the connector recover its own view of table
  structure after a restart; it must never be deleted or compacted away.

## Duplicate prevention downstream

Every CDC-derived record carries a natural idempotency key: `(table, pk, lsn)` — or the
business `event_id` once Flink has transformed it. Sinks use it as an upsert / dedup key.
Details in [`idempotency.md`](idempotency.md).

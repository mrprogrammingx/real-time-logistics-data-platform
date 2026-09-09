# Interview questions — Phase 3 (CDC, Debezium, logical replication)

Answerable from `services/cdc`, `services/connect`, `kafka-connect/postgres-source.json`,
`V3__cdc_replica_identity.sql`.

## Why CDC

* **CDC vs dual-write / outbox:** a dual-write (commit, then publish) can lose an event on
  a crash between the two, or publish one that was rolled back. CDC reads the **WAL** — the
  same durable record that makes the commit durable — so the stream can't diverge from the
  DB. (The transactional outbox pattern also solves it; CDC needs no app change.)
* **Where does ordering come from?** Per row: Debezium keys by PK ⇒ all changes to one row
  are ordered on one partition. Across rows/tables: only `source.lsn` — arrival order means
  nothing.

## PostgreSQL / logical replication

* **`wal_level = logical`** — makes Postgres write enough to the WAL to *reconstruct* row
  changes (not just redo them). Costs some WAL volume.
* **Replication slot** — durable server-side cursor. Postgres will **not** recycle WAL past
  the slot's `confirmed_flush_lsn`, so a dead/stopped connector with a live slot fills the
  disk. `make cdc-slot` shows `active` + `retained`.
* **Publication** — the set of tables streamed. `publication.autocreate.mode = filtered`
  lets Debezium manage it; in production you pre-create it and use a non-superuser role
  with `REPLICATION`.
* **`pgoutput`** — Postgres's built-in logical-decoding plugin; no `wal2json` / `decoderbufs`
  to install.
* **`REPLICA IDENTITY`** — `DEFAULT` puts only the PK in the `before` image on UPDATE/DELETE;
  **`FULL`** puts the whole old row. Phase 3 sets `FULL` on the captured tables so consumers
  can diff old vs new. Cost: bigger WAL.

## Snapshot vs streaming

* **Initial snapshot** (`op: "r"`) — one consistent `SELECT *` per table at a single LSN,
  establishing a baseline; then **streaming** (`c`/`u`/`d`) tails the WAL from that LSN.
* **Incremental snapshot** (signal-triggered, DBLog watermarking) — add or re-snapshot a
  table *without* stopping streaming; snapshot chunks are interleaved with live changes.
  Use it for large tables and for adding `customers`/`restaurants` later.
* `snapshot.mode`: `initial` (default), `never`, `initial_only`, `when_needed`,
  `no_data` (schema only).

## The event

* Envelope: `op`, `ts_ms`, `source{lsn,txId,ts_ms,snapshot,table}`, `before`, `after`.
* **Delete** → one event `{op:d, before:{...}, after:null}` immediately followed by a
  **tombstone** (`value == null`) so a compacted topic can drop the key. `PostgresCdcIT`
  asserts both.
* `CdcRecord` / `CdcEnvelopeParser` in `services/cdc` model this.

## Connect / recovery

* **Distributed vs standalone Connect** — distributed stores connector config, offsets and
  status in Kafka topics and rebalances tasks across workers; standalone keeps them in
  local files (fine for a laptop, no HA).
* **Connector restart** — resumes from the last committed offset (an LSN) in
  `_connect-offsets`; anything produced but not yet committed is **replayed** ⇒ downstream
  must be idempotent. `services/cdc`'s embedded engine uses a `FileOffsetBackingStore` for
  the same purpose.
* **Worker dies** — its tasks rebalance to surviving workers, resume from the same offset,
  no LSN gap.
* **Postgres restarts** — the slot persists; the connector reconnects from
  `confirmed_flush_lsn`.
* **Schema history** (MySQL etc.) — Postgres doesn't need a schema-history topic (DDL comes
  through `pgoutput`), but the `_connect-*` topics still must never be deleted.

## Schema evolution downstream

* Additive column → Debezium emits a new Avro schema version; Schema Registry `BACKWARD`
  compat ⇒ old consumers ignore it. (Same story as Phase 2's `DriverLocation` v1→v2.)
* Rename/drop/type-narrow → expand-then-contract: add new column, dual-write, migrate
  consumers, drop old column later.

## Idempotency (leads into Phase 5)

Every CDC record has a natural key: `(table, pk, lsn)`. Sinks upsert / dedup on it, so the
replay after a connector or Flink restart collapses to one logical row. See
[`../../architecture/idempotency.md`](../../architecture/idempotency.md).

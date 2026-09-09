# Kafka Connect / Debezium

The production CDC path: distributed Kafka Connect running the Debezium PostgreSQL
connector. (For the light-weight embedded-engine version used in tests, see
`services/cdc`.)

- **Image** — `services/connect/Dockerfile` = `confluentinc/cp-kafka-connect:7.8.0` + the
  Debezium PostgreSQL connector plugin (`3.0.8.Final`, pulled from Maven Central).
- **Service** — `kafka-connect` in `docker-compose.yml`, REST API on
  http://localhost:18083, distributed mode (config/offset/status stored in Kafka topics).
- **Connector config** — [`postgres-source.json`](postgres-source.json).

## Use

```bash
make up                 # brings up kafka-connect too
make cdc-register       # POST postgres-source.json to the Connect REST API
make cdc-status         # connector + task state
make cdc-slot           # PostgreSQL replication-slot health
make cdc-tail           # stream flowfleet.orders.cdc as JSON
make cdc-demo           # change an order via the API, watch the event land
```

`scripts/connect.sh` also has `list | plugins | restart | pause | resume | delete | offsets`.

## What the config does

| Setting | Why |
|---|---|
| `plugin.name = pgoutput` | native logical-decoding output plugin (no `wal2json` install) |
| `slot.name`, `publication.name` | durable server-side state; the slot holds WAL until Debezium confirms consumption |
| `publication.autocreate.mode = filtered` | Debezium creates a publication for exactly the `table.include.list` |
| `snapshot.mode = initial` | one consistent `SELECT *` pass (op `r`), then stream the WAL |
| `tombstones.on.delete = true` | emit a null-value record after each delete for log-compaction |
| `decimal.handling.mode = string` | `NUMERIC` → string, not Avro `bytes` — far easier downstream |
| `RegexRouter` | `flowfleet.public.orders` → `flowfleet.orders.cdc` |
| `heartbeat.interval.ms = 10000` | advances `confirmed_flush_lsn` even when the captured tables are idle, so the slot doesn't pin WAL forever |
| `AvroConverter` + Schema Registry | envelope + key schemas registered per topic |

## Operational notes

- **Never** delete the replication slot while the connector is stopped and expected to
  resume — you lose the resume point. **Do** drop an abandoned slot (`make cdc-slot` shows
  `active = f` and growing `retained`) or it fills the disk.
- The internal `_connect-*` topics and the Debezium schema-history topic must not be
  deleted or aggressively compacted.
- In a managed database, replace the superuser with a dedicated role that has
  `REPLICATION` + `SELECT` on the captured tables, and pre-create the publication.

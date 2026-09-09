# PostgreSQL

The operational schema is **owned by `services/api`** and applied with Flyway. There is no
loose SQL to run by hand.

* Migrations: [`../../services/api/src/main/resources/db/migration/`](../../services/api/src/main/resources/db/migration/)
  * `V1__init_schema.sql` — PostGIS extension + core tables (customers, vehicles,
    restaurants, drivers, driver_shifts, orders, order_items, deliveries, geofences),
    indexes, constraints.
  * `V2__seed_reference_data.sql` — a handful of customers/vehicles/restaurants/drivers and
    four geofences around central Yerevan.
* Applied automatically on API startup (`spring.flyway.enabled=true`) and in Testcontainers
  integration tests — so the test schema and the runtime schema are always identical.

## Poke around

```bash
make psql
# or:
psql postgresql://flowfleet:flowfleet@localhost:15432/flowfleet
```

```sql
\dt
SELECT id, name, ST_AsText(geom) FROM geofences;
SELECT id, status, total_amount FROM orders ORDER BY id;
-- which geofence(s) contain a point?
SELECT name FROM geofences
WHERE ST_Contains(geom, ST_SetSRID(ST_MakePoint(44.5133, 40.1776), 4326));
```

## CDC readiness (Phase 3)

`docker-compose.yml` already starts Postgres with `wal_level=logical`,
`max_wal_senders=10`, `max_replication_slots=10`, so Debezium can be pointed at it without
a restart. See [`../../architecture/cdc-design.md`](../../architecture/cdc-design.md).

## Later

* `database/timescaledb/` — hypertables for `driver_locations`, `driver_speed`,
  `delivery_metrics` (Phase 5).
* `database/clickhouse/` — `MergeTree` / `ReplacingMergeTree` DDL for event-level analytics
  (Phase 5).

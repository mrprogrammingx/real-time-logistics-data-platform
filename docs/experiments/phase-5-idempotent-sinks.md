# Experiment: idempotent sinks under replay

**Setup:** `make up` (brings up TimescaleDB + ClickHouse), then
`make flink-submit-all` (4 processing jobs + 2 sink jobs). Generator is producing.

## 1. Data is flowing

```bash
make timescale-peek     # row counts + latest positions
make clickhouse-peek    # deduplicated counts (uses FINAL)
```

TimescaleDB `driver_state` grows ~200 rows/s (one per GPS sample); `driver_speed_windows`
gets one row per driver per minute. ClickHouse `geofence_events` / `delivery_alerts` fill
as simulated drivers cross fences / trip anomaly rules.

## 2. Idempotency on Flink restart (the real test)

```bash
./scripts/flink.sh list
# note the timescale-sink job id, then:
docker compose exec flink-jobmanager flink cancel <timescale-sink-id>

# capture counts BEFORE reprocessing
make timescale-peek

# resubmit — with no savepoint it re-reads from the group's committed offset;
# for a clean "replay everything" test, use --fromSavepoint or reset the group:
docker compose exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 \
  --group flowfleet.flink.timescale-sink --topic flowfleet.driver.state \
  --reset-offsets --to-earliest --execute
./scripts/flink.sh submit timescale-sink

# after it catches up — counts should be UNCHANGED
make timescale-peek
```

`driver_state` row count is identical: every re-inserted `(driver_id, event_time)` hits
`ON CONFLICT DO NOTHING`.

## 3. ClickHouse dedup

```bash
docker compose exec clickhouse clickhouse-client -u flowfleet --password flowfleet -q \
  "SELECT count() raw, uniqExact(event_id) unique FROM flowfleet.geofence_events"
```

After a replay `raw > unique`; `... FROM flowfleet.geofence_events FINAL` gives `unique`.
Force a merge with `OPTIMIZE TABLE flowfleet.geofence_events FINAL` and `raw` drops to
`unique`.

## 4. Sink batching / latency

In the Flink UI (:18086) open the `timescale-sink` job → the sink operator's
`numRecordsIn` climbs in steps of ≤500 (the batch size), and rows appear in TimescaleDB
within ~1 s of being produced (the batch interval).

## Results (fill in from your run)

```
date:
driver_state rows before replay:  ___     after:  ___   (equal)
geofence_events raw / unique after replay:  ___ / ___
sink lag (produced -> visible in DB):  ~___ s
```

## Covered by an automated test

`TimescaleSinkIT` and `ClickHouseSinkIT` (both MiniCluster + Testcontainers) do exactly
this: write a batch, replay the identical batch, assert one logical row survives.

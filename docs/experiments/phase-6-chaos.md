# Experiment: failure engineering

**Setup:** `make up && make cdc-register && make flink-submit-all`, wait ~1 min for the
jobs to build state. Grafana: http://localhost:13000/d/flowfleet-flink · Flink UI :18086.

## 1. Kill a TaskManager → recover from checkpoint

```bash
make chaos-kill-tm
```

The script records metrics, `docker kill`s a TaskManager, polls until the job is `RUNNING`
again, then records metrics after.

**Expect:** job goes `RUNNING → RESTARTING → RUNNING`; `numRestarts` +1; the Kafka source
rewinds to the offsets *in the last checkpoint*, so a tail of records is **reprocessed**.
Then check the sinks:

```bash
make timescale-peek     # driver_state row count should NOT have jumped
```

Because the sink is idempotent (`ON CONFLICT DO NOTHING` on `(driver_id, event_time)`), the
reprocessed records collapse — recovery is exactly-once *as observed*.

## 2. Slow sink → backpressure

```bash
make chaos-slow-sink     # resubmits timescale-sink with --slow-map-ms 20
```

In Grafana, on the **Backpressure** panel:

```
slow-map busyTimeMsPerSecond → ~1000
   └─ upstream operators backPressuredTimeMsPerSecond rises
      └─ Kafka source lag climbs (Kafka source lag panel)
         └─ checkpoint duration grows (unaligned checkpoints keep it bounded)
```

**Fixes to try** (resubmit with each): `--slow-map-ms 0` (remove it), raise sink
parallelism, larger JDBC batch, more source partitions. Record lag-recovery time for each.

## 3. Malformed events → DLQ

```bash
make chaos-malformed     # produces 5 non-Avro messages onto flowfleet.driver.locations
```

**Expect:** the jobs keep running (no restart — `ResilientLocationDeserializer` never
throws); 5 `"reason":"decode-failure"` records appear on `flowfleet.driver.locations.dlq`
with the offending Kafka `partition`/`offset`. In Grafana the **Ingest valid vs DLQ** panel
shows a `dlq` blip.

```bash
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic flowfleet.driver.locations.dlq --from-beginning --timeout-ms 4000 | tail
```

## 4. Duplicate events → one logical row

```bash
make chaos-duplicate     # cancels the sink, resets its group to earliest, resubmits
```

**Expect:** `driver_state` row count identical before and after the full replay.

## 5. Out-of-order / late data

Automated: `WatermarkLatenessIT` feeds a sample behind the watermark and asserts it lands
on `Tags.LATE_LOCATIONS`, not in the window. Live: lower `Watermarks.OUT_OF_ORDERNESS`,
redeploy `driver-speed`, and watch `flowfleet.driver.locations.dlq` for `"late":true`.

## Results (fill in from your run)

```
date:
1. kill-tm:   downtime ___ s | restarts +1 | driver_state rows before/after: ___ / ___ (equal)
2. slow-sink 20ms:  lag peak ___ | checkpoint duration ___ ms | recovery after removing: ___ s
3. malformed x5:    dlq decode-failure count: ___ | job restarts: 0
4. duplicate:       rows before/after: ___ / ___ (equal)
```

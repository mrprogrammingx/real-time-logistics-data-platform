# Flink design

> Status: **implemented** (Phase 4). One module (`flink/`), Java 17 / Flink 1.20, one fat
> jar, one entry class per job. Shares `services/common` (haversine `GeoPoint`) + the Avro
> schemas. Every stateful function has a Flink test-harness / MiniCluster test — no Kafka
> needed to prove the logic.

## Jobs (Phase 4)

| Entry class | Input | Key | Core Flink feature | Output |
|-------------|-------|-----|--------------------|--------|
| `driverstate.DriverStateJob` | `driver.locations` | `driver_id` | `KeyedProcessFunction` + `ValueState` + event-time timer | `flowfleet.driver.state` |
| `speed.DriverSpeedJob` | `driver.locations` | `driver_id` | event-time `TumblingEventTimeWindows` + `ProcessWindowFunction`, late-data side output | `flowfleet.driver.speed-windows` |
| `geofence.GeofenceJob` | `driver.locations` + broadcast geofences | `driver_id` | `KeyedBroadcastProcessFunction` + JTS point-in-polygon | `flowfleet.driver.geofence-events` |
| `anomaly.AnomalyJob` | `driver.locations` | `driver_id` | `KeyedProcessFunction` + timers | `flowfleet.delivery.alerts` |
| `GpsGuard` (in every job) | raw stream | — | `ProcessFunction` side output | valid stream + `flowfleet.driver.locations.dlq` |

**Not yet built** (Phase 4.5 / later): `delivery-analytics` (interval join of
`deliveries.cdc` × geofence events → `delivery.events`), `ORDER_PICKUP_TIMEOUT` (timer on
`deliveries.cdc`). The geofence job currently broadcasts a bundled `geofences.json`; the
production wiring is a `flowfleet.geofences.cdc` broadcast source (same
`GeofenceFunction`).

## Driver state (keyed state)

```
keyBy(driver_id) ──► KeyedProcessFunction
                     state:
                       ValueState<DriverSnapshot>  { lastLocation, prevLocation,
                                                     currentOrderId, shiftStatus,
                                                     currentGeofenceId, lastEventTs }
                     on location  : update lastLocation, derive instantaneous speed/heading
                     on shift evt : update shiftStatus
                     on delivery  : set/clear currentOrderId
```

* State backend: **RocksDB**, incremental checkpoints (state is large: 10k+ keys × snapshot).
* TTL on the snapshot state (e.g. 12h) so drivers that go dark are eventually GC'd.

## Event time & watermarks

GPS arrives out of order (device buffering, mobile network):

```
device ts:  10:00:01   10:00:02   10:00:04   10:00:03   10:00:05
```

* **Event time**, not processing time — a late sample must still land in the right window.
* `WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))` on `DriverLocation.eventTime`.
* `withIdleness(Duration.ofSeconds(30))` so a silent partition doesn't hold the watermark.
* Samples later than the watermark → **side output** `late-locations`, counted, not dropped.

## Windows

| Metric | Window | Type |
|--------|--------|------|
| avg / max speed per driver | 5 min | sliding, 1 min slide |
| deliveries completed | 1 min | tumbling |
| active drivers (distinct keys with a sample) | 30 s | sliding, 10 s slide |
| pickup→delivered latency | session / interval join | per delivery |

## Timers (anomaly-detection & SLA)

```
DRIVER_ASSIGNED(order 9382, driver 123, t0)
        └─ ctx.timerService().registerEventTimeTimer(t0 + 10min)

onTimer:  if state.pickedUp == false  ->  emit ORDER_PICKUP_TIMEOUT to delivery.alerts
on ORDER_PICKED_UP:  state.pickedUp = true; delete timer
```

Also: GPS-gap timer (no sample for 90 s while `ON_DELIVERY`), impossible-speed check
(> 150 km/h between consecutive samples).

## Side outputs / DLQ

Every source-adjacent `ProcessFunction` splits its input:

```
                    ┌─ main  ─► parse OK & DriverLocation.isValid()  ─► downstream
raw bytes ─► parse ─┤
                    └─ dlq   ─► { original, error, topic, partition, offset, ts } ─► *.dlq
```

`DriverLocation` is intentionally lenient (raw doubles, nullable kinematics) so a bad
sample deserializes and can be *inspected* on the DLQ rather than killing the job.

## Checkpointing & recovery

```
checkpointing.interval            = 30 s
checkpointing.timeout             = 2 min
checkpointing.min-pause           = 10 s
checkpointing.mode                = EXACTLY_ONCE      (state)
checkpointing.unaligned           = true              (helps under backpressure)
externalized-checkpoints          = RETAIN_ON_CANCELLATION
restart-strategy                  = exponential-delay
state.backend                     = rocksdb (incremental)
```

* **Checkpoint** = automatic, for failure recovery.
* **Savepoint** = manual, for upgrades / rescale (Flink K8s Operator triggers one before a
  job upgrade, restores from it after — see [`deployment.md`](deployment.md) and the
  Phase 7 savepoint-upgrade experiment).
* Phase 6 experiment: `kill -9` a TaskManager mid-processing → job restarts from last
  checkpoint → verify no lost updates and (with idempotent sinks) no visible duplicates.

## Backpressure (Phase 6 experiment)

Introduce an artificially slow sink (`Thread.sleep` per batch). Expected chain:

```
slow sink ─► sink subtask busy 100% ─► input buffers fill ─► backpressure propagates upstream
          ─► source slows ─► Kafka consumer lag ↑ ─► checkpoint duration ↑ (alignment) ─► maybe checkpoint timeout
```

Fixes measured: sink batching, `sink.parallelism`, connection pool size, async I/O
(`AsyncFunction`), unaligned checkpoints, increasing source partitions/parallelism.

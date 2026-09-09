# Interview questions — Phase 4 (Flink: state, event time, windows, timers, broadcast)

Answerable from `flink/` (`DriverStateFunction`, `SpeedWindowFunction`, `GeofenceFunction`,
`AnomalyFunction`, `GpsGuard`) and their test-harness tests.

## Keyed state

* **What is keyed state?** State partitioned by the `keyBy` key — each key's state lives on
  exactly one subtask, and a function only ever sees the state for the key of the record it
  is processing. `DriverStateFunction` keeps a `ValueState<DriverStateAcc>` per `driver_id`.
* **How does Flink manage it?** In the configured state backend (heap or RocksDB);
  checkpointed to durable storage; on rescale the key groups are redistributed across the
  new subtasks.
* **Why a plain POJO for the accumulator, not a `record` or Avro?** POJO serializer →
  no Kryo, and Flink can evolve the schema (add a field) without a state migration.

## Event time & watermarks

* **Event time vs processing time?** Event time = the timestamp in the data
  (`DriverLocation.eventTime`, the device clock). A sample buffered on a phone for 4 s must
  still be counted in the minute it happened, not the minute Flink received it.
* **What is a watermark?** A claim that "no more events with timestamp ≤ W will arrive".
  It's what lets an event-time window know it can fire. `Watermarks.forDriverLocation()` =
  `forBoundedOutOfOrderness(5s)` (tolerate 5 s of reordering) `+ withIdleness(30s)` (a
  silent partition must not freeze the watermark).
* **Late events?** Anything past the window's watermark → `sideOutputLateData` →
  `Tags.LATE_LOCATIONS`. Counted, routed to the DLQ topic, never dropped.

## Windows

* `DriverSpeedJob`: `TumblingEventTimeWindows.of(1 min)` per driver. `SpeedWindowFunction`
  is a `ProcessWindowFunction` — it buffers the window's ~60 samples, sorts by event time,
  then sums leg distances / derives speed from distance÷time (order-correct; an
  `AggregateFunction` would see arrival order).
* **Tumbling vs sliding vs session?** Tumbling = fixed, non-overlapping. Sliding = fixed
  size, steps by a smaller slide (one event in several windows). Session = gap-based.
* **Pre-aggregation:** `AggregateFunction` + `ProcessWindowFunction` keeps only the
  accumulator in state instead of every element — used when the window is large.

## Timers

* `KeyedProcessFunction` registers **event-time** timers via
  `ctx.timerService().registerEventTimeTimer(deadline)`; they fire when the watermark
  passes the deadline. `DriverStateFunction` re-arms a `+120 s` timer on every sample and
  deletes the previous one; if it ever fires, the driver went silent → state is cleared.
  `AnomalyFunction` uses the same pattern to emit a `GPS_GAP` alert.
* **Event-time vs processing-time timers?** Event-time fires on watermark progress
  (deterministic, replayable); processing-time fires on wall clock (for SLAs measured in
  real time, e.g. a 10-minute pickup deadline).

## Broadcast state

* `GeofenceFunction` is a `KeyedBroadcastProcessFunction`: geofence polygons fan out to
  **every** subtask (`processBroadcastElement` → broadcast `MapState`), while each driver's
  keyed state (`insideState`) remembers which fences they were in, so only ENTER/EXIT
  transitions are emitted.
* The JTS `PreparedGeometry` is rebuilt per subtask from the WKT string and cached — never
  serialized into state.
* **Why broadcast, not a join?** The geofence set is small and changes rarely; broadcasting
  avoids a shuffle and gives every subtask a local copy.

## Side outputs / DLQ

* `GpsGuard` (a `ProcessFunction` before `keyBy`) splits the stream: structurally sane
  samples flow on, the rest go to `Tags.DLQ_LOCATIONS` with a reason. A sample too broken
  to Avro-decode fails at the Kafka source instead (the deserializer's error policy).

## Checkpoints / recovery (exercised in Phase 6)

* **Checkpoint** = automatic, for failure recovery. **Savepoint** = manual, for
  upgrades/rescale.
* Kafka source offsets are stored *in the checkpoint*, not committed to Kafka for
  correctness — so a TaskManager restart replays exactly from the checkpoint, and (with
  idempotent sinks) downstream sees no duplicates.

## Why Java 17 here?

Flink 1.20's runtime targets a Java 17 JVM, so the job jar — and `flowfleet-common` /
`flowfleet-events` which it consumes — compile to Java 17 bytecode. The Spring services
stay on 21. A Java-21 module can depend on a Java-17 jar; not the reverse.

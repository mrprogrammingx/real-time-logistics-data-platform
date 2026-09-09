# Experiment: event time, watermarks, windows, geofences

**Setup:** `make up` (brings up the Flink session cluster + generator), then
`make flink-submit-all`. Flink UI: http://localhost:18086.

## 1. The jobs are running

```bash
make flink-list
```

Expect four RUNNING jobs: `flowfleet-driver-state`, `flowfleet-driver-speed`,
`flowfleet-geofence`, `flowfleet-anomaly`. In the UI, watch the operators and the
watermark on the window operator advance.

## 2. Enriched driver state

```bash
make flink-tail        # flowfleet.driver.state
```

Each `DriverSnapshot` carries `derivedSpeedKph` (from the position delta over the
event-time gap), `tripDistanceMeters`, `sampleCount`, and `processingLagMs`
(now − eventTime). Compare `derivedSpeedKph` with `reportedSpeedKph`.

## 3. Speed windows

```bash
docker compose exec schema-registry kafka-avro-console-consumer \
  --bootstrap-server kafka:29092 --property schema.registry.url=http://localhost:8085 \
  --topic flowfleet.driver.speed-windows
```

One record per driver per minute: `windowStart` / `windowEnd` aligned to the minute,
`avgSpeedKph`, `maxSpeedKph`, `distanceMeters`. New records appear ~1 minute behind real
time (the window can't close until the watermark passes `windowEnd`).

## 4. Geofence transitions

```bash
docker compose exec schema-registry kafka-avro-console-consumer \
  --bootstrap-server kafka:29092 --property schema.registry.url=http://localhost:8085 \
  --topic flowfleet.driver.geofence-events
```

`ENTER` / `EXIT` for the restaurant pickup boxes and the Kentron zone, as simulated drivers
drive their restaurant → customer routes. Only transitions — a driver sitting inside a
fence produces nothing.

## 5. Out-of-order & late data

The generator emits in order, so to see the late-data path either (a) lower
`Watermarks.OUT_OF_ORDERNESS` and redeploy, or (b) inspect `DriverSpeedJobIT`, which feeds
deliberately reordered samples and asserts they still land in the right window. Anything
past the watermark lands on `flowfleet.driver.locations.dlq` tagged `"late": true`.

## 6. Anomalies

```bash
make cdc-tail  # no — use:
docker compose exec schema-registry kafka-avro-console-consumer \
  --bootstrap-server kafka:29092 --property schema.registry.url=http://localhost:8085 \
  --topic flowfleet.delivery.alerts
```

`GPS_GAP` when a simulated driver's stream stops for >120 s of event time;
`IMPOSSIBLE_SPEED` if two consecutive samples imply >150 km/h.

## Results (fill in from your run)

```
date:                       flink: 1.20.1 / java 17
jobs running:               4/4
driver.state records/s:     ___
speed-window lag behind real time:  ~___ s
geofence ENTER/EXIT seen:    yes / no
checkpoint duration (UI):    ___ ms   size: ___
```

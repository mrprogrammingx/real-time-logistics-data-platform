# Avro schemas

Canonical `.avsc` files for every event the platform produces. They live at the repo root
so both the Java build and non-Java tooling (schema-registry registration, `jq`) can use
them.

| File | Java class | Producer | Topic |
|------|-----------|----------|-------|
| `driver-location.avsc` | `com.flowfleet.events.avro.DriverLocation` | `location-generator` (Phase 2) | `flowfleet.driver.locations` |
| `geofence-event.avsc` | `GeofenceEvent` | Flink geofencing job (Phase 4) | `flowfleet.driver.geofence-events` |
| `delivery-event.avsc` | `DeliveryEvent` | Flink delivery-analytics (Phase 4) | `flowfleet.delivery.events` |
| `delivery-alert.avsc` | `DeliveryAlert` | Flink anomaly/timers (Phase 4) | `flowfleet.delivery.alerts` |

`services/events` runs the `avro-maven-plugin` `schema` goal over this directory and
generates `SpecificRecord` classes into `target/generated-sources/avro`. Consumers,
producers and Flink jobs all depend on `flowfleet-events`.

## Serialization

- **Wire format:** Confluent Avro (`KafkaAvroSerializer` / `KafkaAvroDeserializer`), i.e.
  `magic-byte | 4-byte schema-id | avro-binary`. The schema itself is *not* on the wire —
  consumers fetch it from the registry by id and cache it.
- **Subject naming:** `TopicNameStrategy` → subject `flowfleet.driver.locations-value`.
- **Registry compatibility:** `FULL` (set on the `schema-registry` container). A new schema
  version must be readable by the previous reader *and* able to read the previous writer's
  data.

## Schema evolution: `DriverLocation` v1 → v2

v1 (historical, kept only in `services/events/src/test/resources/avro-history/`):

```
driverId, latitude, longitude, eventTime
```

v2 (current) adds three fields, each `["null", T]` with `"default": null`:

```
+ speedKph        ["null","double"]  default null
+ headingDegrees  ["null","int"]     default null
+ vehicleType     ["null","string"]  default null
```

The `default` is what makes it safe both ways during a rolling deploy:

| | reads v1 data | reads v2 data |
|---|---|---|
| **v1 reader** | ✅ | ✅ ignores the 3 unknown fields (FORWARD) |
| **v2 reader** | ✅ fills `null` for the 3 missing fields (BACKWARD) | ✅ |

Proven by `DriverLocationSchemaEvolutionTest` (pure Avro `SchemaCompatibility`, no
container). What would break `FULL`: adding a field without a default, removing a field,
narrowing a type, renaming without an `alias`.

Live check against a running registry:

```bash
./scripts/schema-registry.sh subjects      # what's registered
./scripts/schema-registry.sh latest        # current DriverLocation schema + id
./scripts/schema-registry.sh check-v1      # is v1 still compatible with current? -> {"is_compatible": true}
```

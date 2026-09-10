-- ============================================================================
-- ClickHouse: event-level analytics sink for the Flink jobs (Phase 5).
--
-- Idempotency without ON CONFLICT: ReplacingMergeTree collapses rows with the
-- same ORDER BY key (event_id), keeping the one with the max `ingested_at`, at
-- merge time. A Flink replay re-inserts the same event_id and it is eventually
-- deduplicated. Reads use FINAL / GROUP BY for exact dedup.
-- ============================================================================

CREATE DATABASE IF NOT EXISTS flowfleet;

CREATE TABLE IF NOT EXISTS flowfleet.geofence_events (
    event_id       String,
    driver_id      Int64,
    geofence_id    Int64,
    geofence_name  String,
    geofence_type  LowCardinality(String),
    transition     LowCardinality(String),
    latitude       Float64,
    longitude      Float64,
    event_time     DateTime64(3),
    ingested_at    DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_id);

CREATE TABLE IF NOT EXISTS flowfleet.delivery_alerts (
    event_id     String,
    kind         LowCardinality(String),
    order_id     Nullable(Int64),
    driver_id    Nullable(Int64),
    detail       String,
    event_time   DateTime64(3),
    ingested_at  DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_id);

-- convenience: deduplicated views
CREATE VIEW IF NOT EXISTS flowfleet.geofence_events_current AS
    SELECT * FROM flowfleet.geofence_events FINAL;
CREATE VIEW IF NOT EXISTS flowfleet.delivery_alerts_current AS
    SELECT * FROM flowfleet.delivery_alerts FINAL;

-- ============================================================================
-- TimescaleDB: operational time-series sink for the Flink jobs (Phase 5).
--
-- Append-only hypertables. Idempotency = the primary key includes the event's
-- own timestamp, so a Flink replay re-inserts the *same* row and
-- `ON CONFLICT DO NOTHING` makes it a no-op. "Current state" is a query, not a
-- mutable row — the Timescale-idiomatic pattern.
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- --- driver state (one row per GPS sample, enriched by DriverStateJob) -------
CREATE TABLE driver_state (
    driver_id            BIGINT           NOT NULL,
    event_time           TIMESTAMPTZ      NOT NULL,
    latitude             DOUBLE PRECISION NOT NULL,
    longitude            DOUBLE PRECISION NOT NULL,
    reported_speed_kph   DOUBLE PRECISION,
    derived_speed_kph    DOUBLE PRECISION,
    heading_degrees      INT,
    vehicle_type         TEXT,
    trip_distance_meters DOUBLE PRECISION NOT NULL DEFAULT 0,
    sample_count         BIGINT           NOT NULL DEFAULT 0,
    processing_lag_ms    BIGINT           NOT NULL DEFAULT 0,
    ingested_at          TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (driver_id, event_time)
);
SELECT create_hypertable('driver_state', 'event_time', chunk_time_interval => INTERVAL '1 hour');
CREATE INDEX idx_driver_state_driver_time ON driver_state (driver_id, event_time DESC);

-- --- per-driver speed windows (DriverSpeedJob) ------------------------------
CREATE TABLE driver_speed_windows (
    driver_id       BIGINT           NOT NULL,
    window_start    TIMESTAMPTZ      NOT NULL,
    window_end      TIMESTAMPTZ      NOT NULL,
    sample_count    BIGINT           NOT NULL,
    avg_speed_kph   DOUBLE PRECISION NOT NULL,
    max_speed_kph   DOUBLE PRECISION NOT NULL,
    distance_meters DOUBLE PRECISION NOT NULL,
    ingested_at     TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (driver_id, window_start)
);
SELECT create_hypertable('driver_speed_windows', 'window_start', chunk_time_interval => INTERVAL '1 day');

-- latest known position per driver
CREATE OR REPLACE VIEW driver_current_state AS
SELECT DISTINCT ON (driver_id) *
FROM driver_state
ORDER BY driver_id, event_time DESC;

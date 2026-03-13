-- ============================================================
-- ChargeSquare — ClickHouse schema
-- ============================================================
-- Run automatically on container start via docker-entrypoint-initdb.d

CREATE DATABASE IF NOT EXISTS chargesquare;

-- ─────────────────────────────────────────────────────────
-- Core events table
-- ─────────────────────────────────────────────────────────
-- MergeTree with monthly partitioning and 90-day TTL.
-- Sorted by (timestamp, network_id, station_id) for the
-- most common access patterns (time-range + CPO + station).
-- LowCardinality wraps high-repetition string columns to
-- enable dictionary compression and faster GROUP BY.

CREATE TABLE IF NOT EXISTS chargesquare.charging_events
(
    -- Identity
    event_id         String,
    event_type       LowCardinality(String),
    timestamp        DateTime64(3, 'UTC'),

    -- Station topology
    station_id       LowCardinality(String),
    connector_id     UInt8,
    session_id       String,
    network_id       LowCardinality(String),

    -- Location
    city             LowCardinality(String),
    country          LowCardinality(String),
    latitude         Float32,
    longitude        Float32,

    -- Hardware
    charger_type     LowCardinality(String),
    power_kw         Float32,

    -- Session measurements
    energy_kwh       Float32,
    soc_percent      UInt8,
    vehicle_type     LowCardinality(String),
    duration_minutes UInt16,

    -- Commercial
    price_per_kwh    Float32,
    revenue_eur      Float32,

    -- Operational
    status           LowCardinality(String),
    error_code       LowCardinality(String),
    component        LowCardinality(String),

    -- Electrical measurements (METER_UPDATE only)
    voltage_v        Float32,
    current_a        Float32,

    -- Vehicle identity
    vehicle_brand    LowCardinality(String),
    vehicle_model    LowCardinality(String),
    vehicle_ev_id    String,

    -- Tariff
    tariff_id        LowCardinality(String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, network_id, station_id)
TTL toDateTime(timestamp) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;


-- ─────────────────────────────────────────────────────────
-- Materialized view: hourly session aggregates
-- ─────────────────────────────────────────────────────────
-- Populated incrementally as session_end events arrive.
-- Queries for hourly / daily trend charts read this view
-- instead of the raw table — sub-second response at scale.

CREATE MATERIALIZED VIEW IF NOT EXISTS chargesquare.sessions_hourly_mv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, network_id, city, country, charger_type)
AS
SELECT
    toStartOfHour(timestamp) AS hour,
    network_id,
    city,
    country,
    charger_type,
    count()                  AS session_count,
    sum(energy_kwh)          AS total_energy_kwh,
    sum(revenue_eur)         AS total_revenue_eur,
    sum(duration_minutes)    AS total_duration_minutes,
    sum(power_kw)            AS sum_power_kw
FROM chargesquare.charging_events
WHERE event_type = 'session_stop'
GROUP BY hour, network_id, city, country, charger_type;


-- ─────────────────────────────────────────────────────────
-- Materialized view: daily station performance
-- ─────────────────────────────────────────────────────────

CREATE MATERIALIZED VIEW IF NOT EXISTS chargesquare.station_daily_mv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (day, station_id, network_id)
AS
SELECT
    toDate(timestamp)     AS day,
    station_id,
    network_id,
    city,
    country,
    charger_type,
    count()               AS session_count,
    sum(energy_kwh)       AS total_energy_kwh,
    sum(revenue_eur)      AS total_revenue_eur,
    sum(duration_minutes) AS total_duration_minutes
FROM chargesquare.charging_events
WHERE event_type = 'session_stop'
GROUP BY day, station_id, network_id, city, country, charger_type;


-- ─────────────────────────────────────────────────────────
-- Materialized view: daily error tracking
-- ─────────────────────────────────────────────────────────

CREATE MATERIALIZED VIEW IF NOT EXISTS chargesquare.errors_daily_mv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (day, error_code, network_id)
AS
SELECT
    toDate(timestamp)          AS day,
    error_code,
    network_id,
    count()                    AS error_count,
    count(DISTINCT station_id) AS affected_stations
FROM chargesquare.charging_events
WHERE event_type = 'fault_alert'
  AND error_code != ''
GROUP BY day, error_code, network_id;


-- ─────────────────────────────────────────────────────────
-- Dead-letter & late-events table
-- ─────────────────────────────────────────────────────────
-- Stores events that failed validation or arrived past the
-- watermark.  Useful for tuning the max_lateness window and
-- replaying rejected events in a batch job.

CREATE TABLE IF NOT EXISTS chargesquare.dead_letter_events
(
    rejected_at  DateTime64(3, 'UTC'),
    reason       LowCardinality(String),   -- e.g. "late_event:37s", "missing_field:event_id"
    event_id     String,
    event_type   LowCardinality(String),
    timestamp    Nullable(DateTime64(3, 'UTC')),
    station_id   String,
    raw_event    String                    -- full JSON for replay
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(rejected_at)
ORDER BY (rejected_at, reason)
TTL toDateTime(rejected_at) + INTERVAL 30 DAY;


-- ─────────────────────────────────────────────────────────
-- Materialized view: daily network revenue
-- ─────────────────────────────────────────────────────────

CREATE MATERIALIZED VIEW IF NOT EXISTS chargesquare.network_revenue_daily_mv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (day, network_id)
AS
SELECT
    toDate(timestamp)     AS day,
    network_id,
    count()               AS session_count,
    sum(revenue_eur)      AS total_revenue_eur,
    sum(energy_kwh)       AS total_energy_kwh
FROM chargesquare.charging_events
WHERE event_type = 'session_stop'
GROUP BY day, network_id;

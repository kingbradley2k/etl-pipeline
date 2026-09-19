-- Kenyan Weather ETL Pipeline: PostgreSQL schema
-- Run this file against an empty or existing PostgreSQL database. Statements
-- are idempotent where practical, so development setup can be repeated safely.

BEGIN;

-- One row per configured city. Coordinates are stored with the location so an
-- observation references a stable, normalized location identifier.
CREATE TABLE IF NOT EXISTS locations (
    location_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    country_code CHAR(2) NOT NULL DEFAULT 'KE',
    latitude NUMERIC(8, 5) NOT NULL,
    longitude NUMERIC(8, 5) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT locations_city_country_key UNIQUE (city, country_code),
    CONSTRAINT locations_latitude_range CHECK (latitude BETWEEN -90 AND 90),
    CONSTRAINT locations_longitude_range CHECK (longitude BETWEEN -180 AND 180)
);

-- Each execution is auditable even if an earlier stage fails before loading.
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    records_extracted INTEGER NOT NULL DEFAULT 0,
    records_loaded INTEGER NOT NULL DEFAULT 0,
    records_rejected INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(16) NOT NULL DEFAULT 'running',
    error_message TEXT,
    CONSTRAINT pipeline_runs_status_check
        CHECK (status IN ('running', 'success', 'failed')),
    CONSTRAINT pipeline_runs_counts_nonnegative
        CHECK (
            records_extracted >= 0
            AND records_loaded >= 0
            AND records_rejected >= 0
        ),
    CONSTRAINT pipeline_runs_completion_check
        CHECK (
            (status = 'running' AND completed_at IS NULL)
            OR (status IN ('success', 'failed') AND completed_at IS NOT NULL)
        ),
    CONSTRAINT pipeline_runs_error_check
        CHECK (status = 'failed' OR error_message IS NULL)
);

-- The unique key makes repeated scheduled requests idempotent: the same source
-- cannot create a second observation for one location and observation time.
CREATE TABLE IF NOT EXISTS weather_observations (
    observation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    location_id BIGINT NOT NULL REFERENCES locations(location_id),
    pipeline_run_id BIGINT REFERENCES pipeline_runs(run_id),
    observed_at TIMESTAMPTZ NOT NULL,
    temperature_c NUMERIC(5, 2) NOT NULL,
    humidity_pct NUMERIC(5, 2) NOT NULL,
    wind_speed_kmh NUMERIC(7, 2) NOT NULL,
    pressure_msl_hpa NUMERIC(7, 2),
    weather_code SMALLINT,
    weather_condition VARCHAR(100),
    source VARCHAR(50) NOT NULL DEFAULT 'open-meteo',
    raw_file_path TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT weather_observations_location_time_source_key
        UNIQUE (location_id, observed_at, source),
    CONSTRAINT weather_observations_temperature_range
        CHECK (temperature_c BETWEEN -90 AND 60),
    CONSTRAINT weather_observations_humidity_range
        CHECK (humidity_pct BETWEEN 0 AND 100),
    CONSTRAINT weather_observations_wind_speed_nonnegative
        CHECK (wind_speed_kmh >= 0),
    CONSTRAINT weather_observations_pressure_range
        CHECK (pressure_msl_hpa IS NULL OR pressure_msl_hpa BETWEEN 300 AND 1200)
);

-- Query paths used by the dashboard and analytical SQL.
CREATE INDEX IF NOT EXISTS idx_weather_observations_location_observed_at
    ON weather_observations (location_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_weather_observations_observed_at
    ON weather_observations (observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_weather_observations_pipeline_run_id
    ON weather_observations (pipeline_run_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status_started_at
    ON pipeline_runs (status, started_at DESC);

COMMIT;

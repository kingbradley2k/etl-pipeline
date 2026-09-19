"""Transactional PostgreSQL loading for validated weather observations."""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any

import pandas as pd
from sqlalchemy import Engine, URL, create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from src.config import Settings, get_settings


LOGGER = logging.getLogger(__name__)
REQUIRED_COLUMNS = {
    "location",
    "country_code",
    "latitude",
    "longitude",
    "observed_at",
    "temperature_c",
    "humidity_pct",
    "wind_speed_kmh",
    "pressure_msl_hpa",
    "weather_code",
    "weather_condition",
    "source",
    "raw_file_path",
}

LOCATION_UPSERT = text(
    """
    INSERT INTO locations (city, country_code, latitude, longitude)
    VALUES (:city, :country_code, :latitude, :longitude)
    ON CONFLICT (city, country_code) DO UPDATE
    SET latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        updated_at = CURRENT_TIMESTAMP
    RETURNING location_id
    """
)

OBSERVATION_INSERT = text(
    """
    INSERT INTO weather_observations (
        location_id, pipeline_run_id, observed_at, temperature_c, humidity_pct,
        wind_speed_kmh, pressure_msl_hpa, weather_code, weather_condition,
        source, raw_file_path
    ) VALUES (
        :location_id, :pipeline_run_id, :observed_at, :temperature_c,
        :humidity_pct, :wind_speed_kmh, :pressure_msl_hpa, :weather_code,
        :weather_condition, :source, :raw_file_path
    )
    ON CONFLICT (location_id, observed_at, source) DO NOTHING
    """
)

PIPELINE_RUN_START = text(
    """
    INSERT INTO pipeline_runs (status)
    VALUES ('running')
    RETURNING run_id
    """
)

PIPELINE_RUN_COMPLETE = text(
    """
    UPDATE pipeline_runs
    SET completed_at = CURRENT_TIMESTAMP,
        records_extracted = :records_extracted,
        records_loaded = :records_loaded,
        records_rejected = :records_rejected,
        status = :status,
        error_message = :error_message
    WHERE run_id = :run_id
    """
)


class DatabaseLoadError(RuntimeError):
    """Raised when a database transaction cannot be completed."""


@dataclass(frozen=True)
class LoadResult:
    """Counts returned by one transactional observation load."""

    records_loaded: int
    duplicates_skipped: int


def build_database_url(settings: Settings) -> URL:
    """Build a PostgreSQL URL from environment-backed settings.

    ``URL.create`` safely handles special characters in a local password.
    """
    return URL.create(
        drivername="postgresql+psycopg",
        username=settings.postgres_user,
        password=settings.postgres_password,
        host=settings.postgres_host,
        port=settings.postgres_port,
        database=settings.postgres_db,
    )


class DatabaseLoader:
    """Load validated data and pipeline audit records through SQLAlchemy."""

    def __init__(self, settings: Settings | None = None, engine: Engine | Any | None = None) -> None:
        self.settings = settings or get_settings()
        self.engine = engine or create_engine(
            build_database_url(self.settings),
            pool_pre_ping=True,
        )

    def create_pipeline_run(self) -> int:
        """Create and return an audit row before an ETL run begins."""
        try:
            with self.engine.begin() as connection:
                run_id = connection.execute(PIPELINE_RUN_START).scalar_one()
        except SQLAlchemyError as error:
            raise DatabaseLoadError("Could not create pipeline run.") from error

        LOGGER.info("Created pipeline run %s", run_id)
        return int(run_id)

    def load_observations(self, records: pd.DataFrame, pipeline_run_id: int) -> LoadResult:
        """Upsert locations and insert observations in one database transaction.

        The transaction commits only if all records load successfully. Any
        database error exits the context manager with an exception, causing
        SQLAlchemy to roll back every location and observation change.
        """
        missing_columns = REQUIRED_COLUMNS.difference(records.columns)
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(f"Validated records are missing required columns: {missing}")

        if records.empty:
            LOGGER.info("No validated records to load for pipeline run %s", pipeline_run_id)
            return LoadResult(records_loaded=0, duplicates_skipped=0)

        loaded = 0
        duplicates_skipped = 0
        try:
            with self.engine.begin() as connection:
                for record in records.to_dict(orient="records"):
                    location_id = connection.execute(
                        LOCATION_UPSERT,
                        {
                            "city": record["location"],
                            "country_code": record["country_code"],
                            "latitude": record["latitude"],
                            "longitude": record["longitude"],
                        },
                    ).scalar_one()
                    insert_result = connection.execute(
                        OBSERVATION_INSERT,
                        {
                            "location_id": location_id,
                            "pipeline_run_id": pipeline_run_id,
                            "observed_at": _database_value(record["observed_at"]),
                            "temperature_c": _database_value(record["temperature_c"]),
                            "humidity_pct": _database_value(record["humidity_pct"]),
                            "wind_speed_kmh": _database_value(record["wind_speed_kmh"]),
                            "pressure_msl_hpa": _database_value(record["pressure_msl_hpa"]),
                            "weather_code": _database_value(record["weather_code"]),
                            "weather_condition": _database_value(record["weather_condition"]),
                            "source": record["source"],
                            "raw_file_path": _database_value(record["raw_file_path"]),
                        },
                    )
                    if insert_result.rowcount == 0:
                        duplicates_skipped += 1
                    else:
                        loaded += 1
        except SQLAlchemyError as error:
            LOGGER.exception("Database load failed for pipeline run %s", pipeline_run_id)
            raise DatabaseLoadError("Weather observations were not loaded.") from error

        LOGGER.info("Records loaded: %d", loaded)
        LOGGER.info("Duplicate observations skipped: %d", duplicates_skipped)
        return LoadResult(records_loaded=loaded, duplicates_skipped=duplicates_skipped)

    def complete_pipeline_run(
        self,
        run_id: int,
        *,
        records_extracted: int,
        records_loaded: int,
        records_rejected: int,
        status: str,
        error_message: str | None = None,
    ) -> None:
        """Store final run counts and status in the pipeline audit table."""
        if status not in {"success", "failed"}:
            raise ValueError("Final pipeline status must be 'success' or 'failed'.")
        if status == "failed" and not error_message:
            raise ValueError("A failed pipeline run requires an error message.")
        if min(records_extracted, records_loaded, records_rejected) < 0:
            raise ValueError("Pipeline record counts cannot be negative.")

        try:
            with self.engine.begin() as connection:
                update_result = connection.execute(
                    PIPELINE_RUN_COMPLETE,
                    {
                        "run_id": run_id,
                        "records_extracted": records_extracted,
                        "records_loaded": records_loaded,
                        "records_rejected": records_rejected,
                        "status": status,
                        "error_message": error_message,
                    },
                )
                if update_result.rowcount != 1:
                    raise DatabaseLoadError(f"Pipeline run {run_id} was not found.")
        except SQLAlchemyError as error:
            LOGGER.exception("Could not finalize pipeline run %s", run_id)
            raise DatabaseLoadError("Could not finalize pipeline run.") from error

        LOGGER.info("Pipeline run %s completed with status %s", run_id, status)


def _database_value(value: Any) -> Any:
    """Convert pandas missing values and timestamps to DBAPI-friendly values."""
    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value

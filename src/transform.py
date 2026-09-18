"""Normalize, validate, and report on raw weather API payloads."""

from __future__ import annotations

from dataclasses import dataclass
import logging
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pandas as pd

from src.extract import FetchedWeatherPayload


LOGGER = logging.getLogger(__name__)
SOURCE_NAME = "open-meteo"
CLEAN_COLUMNS = [
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
]
REJECTION_COLUMNS = ["record_index", "location", "reason", "raw_record"]

WEATHER_CONDITIONS = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Depositing rime fog",
    51: "Light drizzle",
    53: "Moderate drizzle",
    55: "Dense drizzle",
    61: "Slight rain",
    63: "Moderate rain",
    65: "Heavy rain",
    71: "Slight snow fall",
    73: "Moderate snow fall",
    75: "Heavy snow fall",
    80: "Slight rain showers",
    81: "Moderate rain showers",
    82: "Violent rain showers",
    95: "Thunderstorm",
}


@dataclass(frozen=True)
class TransformationResult:
    """Validated records, rejected records, and transformation statistics."""

    clean_records: pd.DataFrame
    rejected_records: pd.DataFrame
    records_received: int
    records_cleaned: int
    duplicates_removed: int

    @property
    def records_rejected(self) -> int:
        """Return the number of records excluded for data-quality reasons."""
        return len(self.rejected_records)

    @property
    def records_ready_for_loading(self) -> int:
        """Return the de-duplicated number of clean records."""
        return len(self.clean_records)


def transform_payloads(payloads: Sequence[FetchedWeatherPayload]) -> TransformationResult:
    """Flatten Phase 2 payloads before applying standard transformation rules."""
    flattened_records = [
        _flatten_payload(payload.location.city, payload.location.country_code, payload.payload, payload.raw_file)
        for payload in payloads
    ]
    return transform_records(flattened_records)


def transform_records(records: Sequence[Mapping[str, Any]]) -> TransformationResult:
    """Clean flat weather records and explicitly retain rejected-record reasons.

    This function is intentionally usable with fixture records, allowing quality
    rules to be tested without a live API or raw files.
    """
    clean_rows: list[dict[str, Any]] = []
    rejected_rows: list[dict[str, Any]] = []

    for index, raw_record in enumerate(records):
        cleaned, reasons = _clean_record(raw_record)
        if reasons:
            rejected_rows.append(
                {
                    "record_index": index,
                    "location": cleaned.get("location"),
                    "reason": "; ".join(reasons),
                    "raw_record": dict(raw_record),
                }
            )
            continue
        clean_rows.append(cleaned)

    cleaned_before_deduplication = len(clean_rows)
    clean_dataframe = pd.DataFrame(clean_rows, columns=CLEAN_COLUMNS)
    if not clean_dataframe.empty:
        clean_dataframe = clean_dataframe.drop_duplicates(
            subset=["location", "observed_at", "source"], keep="first"
        ).reset_index(drop=True)
    duplicates_removed = cleaned_before_deduplication - len(clean_dataframe)
    rejected_dataframe = pd.DataFrame(rejected_rows, columns=REJECTION_COLUMNS)

    result = TransformationResult(
        clean_records=clean_dataframe,
        rejected_records=rejected_dataframe,
        records_received=len(records),
        records_cleaned=cleaned_before_deduplication,
        duplicates_removed=duplicates_removed,
    )
    LOGGER.info("Records received: %d", result.records_received)
    LOGGER.info("Records rejected: %d", result.records_rejected)
    LOGGER.info("Records cleaned: %d", result.records_cleaned)
    LOGGER.info("Duplicates removed: %d", result.duplicates_removed)
    LOGGER.info("Records ready for loading: %d", result.records_ready_for_loading)
    return result


def _flatten_payload(
    city: str,
    country_code: str,
    payload: Mapping[str, Any],
    raw_file: Path,
) -> dict[str, Any]:
    """Combine Open-Meteo metadata and its nested current-weather object."""
    current = payload.get("current")
    current_fields = dict(current) if isinstance(current, Mapping) else {}
    return {
        **current_fields,
        "location": city,
        "country_code": country_code,
        "latitude": payload.get("latitude"),
        "longitude": payload.get("longitude"),
        "timezone": payload.get("timezone"),
        "raw_file_path": str(raw_file),
    }


def _clean_record(raw_record: Mapping[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """Coerce one record into the target schema and return all quality failures."""
    record = {_normalize_key(key): value for key, value in raw_record.items()}
    location = _clean_text(_first_value(record, "location", "city"))
    country_code = _clean_text(_first_value(record, "country_code", "country")) or "KE"
    latitude = _number(_first_value(record, "latitude", "lat"))
    longitude = _number(_first_value(record, "longitude", "lon", "lng"))
    temperature = _number(_first_value(record, "temperature_2m", "temperature", "temperature_c"))
    humidity = _number(_first_value(record, "relative_humidity_2m", "humidity", "humidity_pct"))
    wind_speed = _number(_first_value(record, "wind_speed_10m", "wind_speed", "wind_speed_kmh"))
    pressure = _number(_first_value(record, "pressure_msl", "pressure", "pressure_msl_hpa"))
    weather_code = _integer(_first_value(record, "weather_code", "condition_code"))
    observed_at = _timestamp(_first_value(record, "time", "timestamp", "observed_at"), record.get("timezone"))

    condition = _clean_text(_first_value(record, "weather_condition", "condition"))
    if condition is None and weather_code is not None:
        condition = WEATHER_CONDITIONS.get(weather_code, f"Unknown code ({weather_code})")

    cleaned = {
        "location": location,
        "country_code": country_code.upper(),
        "latitude": latitude,
        "longitude": longitude,
        "observed_at": observed_at,
        "temperature_c": temperature,
        "humidity_pct": humidity,
        "wind_speed_kmh": wind_speed,
        "pressure_msl_hpa": pressure,
        "weather_code": weather_code,
        "weather_condition": condition,
        "source": _clean_text(record.get("source")) or SOURCE_NAME,
        "raw_file_path": _clean_text(record.get("raw_file_path")),
    }
    return cleaned, _quality_failures(cleaned)


def _quality_failures(record: Mapping[str, Any]) -> list[str]:
    """Return all failed quality rules for a cleaned record."""
    failures: list[str] = []
    if not record["location"]:
        failures.append("missing location")
    if record["observed_at"] is None:
        failures.append("invalid or missing timestamp")
    _required_numeric_failure(failures, "latitude", record["latitude"])
    _required_numeric_failure(failures, "longitude", record["longitude"])
    _required_numeric_failure(failures, "temperature", record["temperature_c"])
    _required_numeric_failure(failures, "humidity", record["humidity_pct"])
    _required_numeric_failure(failures, "wind speed", record["wind_speed_kmh"])

    if record["latitude"] is not None and not -90 <= record["latitude"] <= 90:
        failures.append("latitude must be between -90 and 90")
    if record["longitude"] is not None and not -180 <= record["longitude"] <= 180:
        failures.append("longitude must be between -180 and 180")
    if record["temperature_c"] is not None and not -90 <= record["temperature_c"] <= 60:
        failures.append("temperature must be between -90 and 60 Celsius")
    if record["humidity_pct"] is not None and not 0 <= record["humidity_pct"] <= 100:
        failures.append("humidity must be between 0 and 100")
    if record["wind_speed_kmh"] is not None and record["wind_speed_kmh"] < 0:
        failures.append("wind speed cannot be negative")
    if record["pressure_msl_hpa"] is not None and not 300 <= record["pressure_msl_hpa"] <= 1_200:
        failures.append("pressure must be between 300 and 1200 hPa")
    return failures


def _required_numeric_failure(failures: list[str], name: str, value: float | None) -> None:
    if value is None:
        failures.append(f"invalid or missing {name}")


def _normalize_key(key: object) -> str:
    return (
        str(key)
        .strip()
        .lower()
        .replace("%", "pct")
        .replace(" ", "_")
        .replace("-", "_")
    )


def _first_value(record: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = record.get(key)
        if value is not None:
            return value
    return None


def _clean_text(value: Any) -> str | None:
    if value is None or isinstance(value, (list, dict, set, tuple)):
        return None
    if pd.isna(value):
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, (bool, list, dict, set, tuple)):
        return None
    parsed = pd.to_numeric(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return float(parsed)


def _integer(value: Any) -> int | None:
    number = _number(value)
    if number is None or not number.is_integer():
        return None
    return int(number)


def _timestamp(value: Any, timezone_name: Any) -> pd.Timestamp | None:
    if value is None:
        return None
    try:
        timestamp = pd.Timestamp(value)
        if pd.isna(timestamp):
            return None
        if timestamp.tzinfo is None:
            timezone_value = str(timezone_name or "Africa/Nairobi")
            timestamp = timestamp.tz_localize(ZoneInfo(timezone_value))
        return timestamp.tz_convert("UTC")
    except (TypeError, ValueError, ZoneInfoNotFoundError):
        return None

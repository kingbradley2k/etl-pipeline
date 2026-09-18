"""Tests for weather-record cleaning and data-quality checks."""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.transform import transform_records


def valid_record(**overrides: Any) -> dict[str, Any]:
    record = {
        "location": " Nairobi ",
        "country_code": " ke ",
        "latitude": "-1.2864",
        "longitude": "36.8172",
        "time": "2026-09-18T12:00",
        "timezone": "Africa/Nairobi",
        "temperature_2m": "25.4",
        "relative_humidity_2m": "68",
        "wind_speed_10m": "12.3",
        "pressure_msl": "1014.2",
        "weather_code": "2",
    }
    record.update(overrides)
    return record


def test_transform_normalizes_types_whitespace_and_weather_condition() -> None:
    result = transform_records([valid_record()])

    assert result.records_ready_for_loading == 1
    row = result.clean_records.iloc[0]
    assert row["location"] == "Nairobi"
    assert row["country_code"] == "KE"
    assert row["temperature_c"] == 25.4
    assert row["weather_condition"] == "Partly cloudy"
    assert row["observed_at"] == pd.Timestamp("2026-09-18T09:00:00Z")


def test_transform_accepts_inconsistent_field_names() -> None:
    record = valid_record(
        **{
            "location": None,
            "city": " Kisumu ",
            "latitude": None,
            "Lat": "-0.1022",
            "longitude": None,
            "LNG": "34.7617",
            "temperature_2m": None,
            "Temperature C": "24",
            "relative_humidity_2m": None,
            "Humidity %": "70",
            "wind_speed_10m": None,
            "Wind Speed": "4",
        }
    )

    result = transform_records([record])

    assert result.records_ready_for_loading == 1
    assert result.clean_records.iloc[0]["location"] == "Kisumu"


def test_transform_rejects_missing_malformed_and_impossible_values() -> None:
    result = transform_records(
        [
            valid_record(location=" "),
            valid_record(temperature_2m="not-a-number"),
            valid_record(relative_humidity_2m=101),
            valid_record(wind_speed_10m=-1),
            valid_record(latitude=91, longitude=181),
            valid_record(time="not-a-timestamp"),
        ]
    )

    assert result.records_ready_for_loading == 0
    assert result.records_rejected == 6
    reasons = " ".join(result.rejected_records["reason"].tolist())
    assert "missing location" in reasons
    assert "invalid or missing temperature" in reasons
    assert "humidity must be between 0 and 100" in reasons
    assert "wind speed cannot be negative" in reasons
    assert "latitude must be between -90 and 90" in reasons
    assert "invalid or missing timestamp" in reasons


def test_transform_removes_duplicate_location_timestamp_records() -> None:
    result = transform_records([valid_record(), valid_record(temperature_2m=30)])

    assert result.records_cleaned == 2
    assert result.duplicates_removed == 1
    assert result.records_ready_for_loading == 1

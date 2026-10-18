"""Phase 10 boundary and cross-stage tests with no live infrastructure."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from src.config import get_settings
from src.extract import ExtractionError, Location, WeatherExtractor
from src.transform import transform_payloads, transform_records


class Response:
    """Minimal successful HTTP response used to mock the API boundary."""

    def __init__(self, payload: dict[str, Any] | None = None, json_error: Exception | None = None) -> None:
        self.payload = payload
        self.json_error = json_error

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        if self.json_error:
            raise self.json_error
        assert self.payload is not None
        return self.payload


class Session:
    """Sequence-based HTTP session fake that never opens a network connection."""

    def __init__(self, responses: list[Response]) -> None:
        self.responses = responses
        self.calls = 0

    def get(self, url: str, **kwargs: object) -> Response:
        self.calls += 1
        return self.responses.pop(0)


def payload() -> dict[str, Any]:
    return {
        "latitude": -1.2864,
        "longitude": 36.8172,
        "timezone": "Africa/Nairobi",
        "current": {
            "time": "2026-09-18T12:00",
            "temperature_2m": 25.4,
            "relative_humidity_2m": 68,
            "wind_speed_10m": 12.3,
            "pressure_msl": 1014.2,
            "weather_code": 2,
        },
    }


def settings_for(tmp_path: Path, **changes: object):
    settings = replace(
        get_settings(),
        raw_data_dir=tmp_path,
        http_max_retries=2,
        http_retry_backoff_seconds=0,
    )
    return replace(settings, **changes)


def test_malformed_json_is_retried_and_never_archived(tmp_path: Path) -> None:
    session = Session([Response(json_error=ValueError("invalid JSON")), Response(json_error=ValueError("invalid JSON"))])
    extractor = WeatherExtractor(settings_for(tmp_path), session=session, sleep=lambda _: None)

    with pytest.raises(ExtractionError, match="invalid JSON"):
        extractor.extract([Location("Nairobi", -1.2864, 36.8172)])

    assert session.calls == 2
    assert list(tmp_path.glob("*.json")) == []


def test_mocked_api_response_flows_from_extraction_to_validated_dataframe(tmp_path: Path) -> None:
    session = Session([Response(payload())])
    extractor = WeatherExtractor(settings_for(tmp_path, http_max_retries=1), session=session)

    extraction = extractor.extract([Location("Nairobi", -1.2864, 36.8172)])
    transformed = transform_payloads(extraction.payloads)

    assert session.calls == 1
    assert extraction.payloads[0].raw_file.exists()
    assert transformed.records_ready_for_loading == 1
    row = transformed.clean_records.iloc[0]
    assert row["location"] == "Nairobi"
    assert row["temperature_c"] == 25.4
    assert row["weather_condition"] == "Partly cloudy"


def test_non_scalar_measurement_is_rejected_instead_of_crashing() -> None:
    record = {
        "location": "Nairobi",
        "latitude": -1.2864,
        "longitude": 36.8172,
        "time": "2026-09-18T12:00",
        "temperature_2m": ["malformed"],
        "relative_humidity_2m": 68,
        "wind_speed_10m": 12.3,
    }

    transformed = transform_records([record])

    assert transformed.records_rejected == 1
    assert transformed.records_ready_for_loading == 0
    assert "invalid or missing temperature" in transformed.rejected_records.iloc[0]["reason"]
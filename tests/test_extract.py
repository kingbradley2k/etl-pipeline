"""Unit tests for Open-Meteo extraction; no test calls the live API."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest
import requests

from src.config import get_settings
from src.extract import ExtractionError, Location, WeatherExtractor


class FakeResponse:
    """Small requests.Response substitute for deterministic unit tests."""

    def __init__(
        self, payload: object | None = None, error: Exception | None = None) -> None:
        self.payload = payload
        self.error = error

    def raise_for_status(self) -> None:
        if self.error:
            raise self.error

    def json(self) -> object:
        if self.error:
            raise self.error
        return self.payload


class FakeSession:
    """Records requests and returns a configured sequence of responses."""

    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = responses
        self.calls: list[dict[str, object]] = []

    def get(self, url: str, **kwargs: object) -> FakeResponse:
        self.calls.append({"url": url, **kwargs})
        return self.responses.pop(0)


def settings_for(tmp_path: Path, **changes: object):
    """Return test settings with local raw storage and no retry delay."""
    settings = replace(
        get_settings(),
        raw_data_dir=tmp_path,
        http_max_retries=2,
        http_retry_backoff_seconds=0,
    )
    return replace(settings, **changes)


def valid_payload() -> dict[str, object]:
    return {"latitude": -1.2864, "longitude": 36.8172, "current": {"time": "2026-09-18T12:00", "temperature_2m": 25.0}}


def test_extract_archives_original_payload_and_returns_structured_data(tmp_path: Path) -> None:
    session = FakeSession([FakeResponse(valid_payload())])
    extractor = WeatherExtractor(settings_for(tmp_path), session=session)

    result = extractor.extract([Location("Nairobi", -1.2864, 36.8172)])

    assert result.records_extracted == 1
    assert result.payloads[0].location.city == "Nairobi"
    assert result.payloads[0].raw_file.exists()
    assert result.payloads[0].raw_file.read_text(encoding="utf-8").startswith("{")
    assert session.calls[0]["timeout"] == 15


def test_extract_retries_a_transient_http_failure(tmp_path: Path) -> None:
    http_error = requests.HTTPError("service unavailable")
    session = FakeSession([FakeResponse(error=http_error), FakeResponse(valid_payload())])
    delays: list[float] = []
    extractor = WeatherExtractor(settings_for(tmp_path), session=session, sleep=delays.append)

    result = extractor.extract([Location("Nairobi", -1.2864, 36.8172)])

    assert result.records_extracted == 1
    assert len(session.calls) == 2
    assert delays == [0]


def test_extract_rejects_malformed_response_after_archiving_it(tmp_path: Path) -> None:
    session = FakeSession([FakeResponse({"latitude": -1.2864})])
    extractor = WeatherExtractor(settings_for(tmp_path), session=session)

    with pytest.raises(ExtractionError, match="missing object 'current'"):
        extractor.extract([Location("Nairobi", -1.2864, 36.8172)])

    assert len(list(tmp_path.glob("weather_nairobi_*.json"))) == 1


def test_extract_raises_after_exhausting_retries(tmp_path: Path) -> None:
    timeout = requests.Timeout("timed out")
    session = FakeSession([FakeResponse(error=timeout), FakeResponse(error=timeout)])
    extractor = WeatherExtractor(settings_for(tmp_path), session=session, sleep=lambda _: None)

    with pytest.raises(ExtractionError, match="after 2 attempts"):
        extractor.extract([Location("Nairobi", -1.2864, 36.8172)])

"""Extract and archive raw current-weather responses from Open-Meteo."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import time
from typing import Any, Callable, Sequence

import requests

from src.config import Settings, get_settings
from src.logging_config import configure_logging


LOGGER = logging.getLogger(__name__)
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
CURRENT_FIELDS = (
    "temperature_2m,relative_humidity_2m,wind_speed_10m,pressure_msl,"
    "weather_code"
)


class ExtractionError(RuntimeError):
    """Raised when one or more configured locations cannot be extracted."""


@dataclass(frozen=True)
class Location:
    """A configured location queried by geographic coordinate."""

    city: str
    latitude: float
    longitude: float
    country_code: str = "KE"


KENYAN_LOCATIONS: tuple[Location, ...] = (
    Location("Kakamega", 0.2827, 34.7519),
    Location("Kisumu", -0.1022, 34.7617),
    Location("Nairobi", -1.2864, 36.8172),
    Location("Mombasa", -4.0435, 39.6682),
)


@dataclass(frozen=True)
class FetchedWeatherPayload:
    """An untransformed API payload and the raw file that preserves it."""

    location: Location
    payload: dict[str, Any]
    raw_file: Path


@dataclass(frozen=True)
class ExtractionResult:
    """The successful raw payloads produced during one extraction run."""

    payloads: list[FetchedWeatherPayload]

    @property
    def records_extracted(self) -> int:
        """Return the number of location responses ready for transformation."""
        return len(self.payloads)


class WeatherExtractor:
    """HTTP client with bounded retries and raw-response archiving."""

    def __init__(
        self,
        settings: Settings | None = None,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.settings = settings or get_settings()
        self.session = session or requests.Session()
        self.sleep = sleep

        if self.settings.http_timeout_seconds <= 0:
            raise ValueError("HTTP_TIMEOUT_SECONDS must be greater than zero.")
        if self.settings.http_max_retries < 1:
            raise ValueError("HTTP_MAX_RETRIES must be at least one.")
        if self.settings.http_retry_backoff_seconds < 0:
            raise ValueError("HTTP_RETRY_BACKOFF_SECONDS cannot be negative.")

    def extract(self, locations: Sequence[Location] = KENYAN_LOCATIONS) -> ExtractionResult:
        """Fetch every location, saving each JSON response before validation.

        A failed location causes the run to fail after all configured locations
        have been attempted. This prevents a later pipeline stage from silently
        loading a partial geographic snapshot.
        """
        successful_payloads: list[FetchedWeatherPayload] = []
        failures: list[str] = []

        for location in locations:
            try:
                payload = self._fetch_location(location)
                raw_file = self._save_raw_response(location, payload)
                self._validate_response_shape(payload)
                successful_payloads.append(
                    FetchedWeatherPayload(location, payload, raw_file)
                )
                LOGGER.info("Extracted weather response for %s", location.city)
            except ExtractionError as error:
                failures.append(f"{location.city}: {error}")
                LOGGER.error("Extraction failed for %s: %s", location.city, error)

        if failures:
            raise ExtractionError("; ".join(failures))

        LOGGER.info("Extracted %d weather responses", len(successful_payloads))
        return ExtractionResult(payloads=successful_payloads)

    def _fetch_location(self, location: Location) -> dict[str, Any]:
        """Request one location with retry handling for transient failures."""
        parameters = {
            "latitude": location.latitude,
            "longitude": location.longitude,
            "current": CURRENT_FIELDS,
            "timezone": self.settings.timezone,
        }

        for attempt in range(1, self.settings.http_max_retries + 1):
            try:
                response = self.session.get(
                    OPEN_METEO_URL,
                    params=parameters,
                    timeout=self.settings.http_timeout_seconds,
                )
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise ExtractionError("API response JSON must be an object.")
                return payload
            except ExtractionError:
                raise
            except (requests.RequestException, ValueError) as error:
                if attempt == self.settings.http_max_retries:
                    raise ExtractionError(
                        f"request failed after {attempt} attempts: {error}"
                    ) from error

                delay = self.settings.http_retry_backoff_seconds * (2 ** (attempt - 1))
                LOGGER.warning(
                    "Attempt %d/%d failed for %s; retrying in %.1f seconds: %s",
                    attempt,
                    self.settings.http_max_retries,
                    location.city,
                    delay,
                    error,
                )
                self.sleep(delay)

        raise AssertionError("Unreachable retry state")

    def _save_raw_response(self, location: Location, payload: dict[str, Any]) -> Path:
        """Persist an unmodified parsed JSON payload with a safe UTC filename."""
        raw_directory = self.settings.raw_data_dir
        raw_directory.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
        filename = f"weather_{location.city.lower()}_{timestamp}.json"
        raw_file = self._available_path(raw_directory / filename)

        with raw_file.open("x", encoding="utf-8") as file_handle:
            json.dump(payload, file_handle, indent=2, sort_keys=True)

        LOGGER.info("Saved raw API response to %s", raw_file)
        return raw_file

    @staticmethod
    def _available_path(candidate: Path) -> Path:
        """Return a non-existing path without overwriting an earlier response."""
        if not candidate.exists():
            return candidate

        for suffix in range(1, 10_000):
            alternative = candidate.with_stem(f"{candidate.stem}_{suffix:02d}")
            if not alternative.exists():
                return alternative
        raise ExtractionError("Could not allocate a unique raw-data filename.")

    @staticmethod
    def _validate_response_shape(payload: dict[str, Any]) -> None:
        """Reject malformed payloads while preserving their archived raw JSON."""
        current = payload.get("current")
        if not isinstance(current, dict):
            raise ExtractionError("malformed API response: missing object 'current'")
        if not current.get("time"):
            raise ExtractionError("malformed API response: missing current.time")


def main() -> None:
    """Run extraction directly while the full ETL orchestrator is not built."""
    settings = get_settings()
    configure_logging(settings.log_level)
    result = WeatherExtractor(settings).extract()
    LOGGER.info("Extraction complete: %d responses archived", result.records_extracted)


if __name__ == "__main__":
    main()

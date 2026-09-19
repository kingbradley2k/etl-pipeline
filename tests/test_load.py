"""Unit tests for transactional database loading without PostgreSQL."""

from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import replace
from typing import Any

import pandas as pd
import pytest
from sqlalchemy.exc import SQLAlchemyError

from src.config import get_settings
from src.load import DatabaseLoadError, DatabaseLoader, build_database_url


class FakeResult:
    def __init__(self, *, scalar: int | None = None, rowcount: int = 1) -> None:
        self.scalar = scalar
        self.rowcount = rowcount

    def scalar_one(self) -> int:
        assert self.scalar is not None
        return self.scalar


class FakeTransaction(AbstractContextManager["FakeConnection"]):
    def __init__(self, connection: "FakeConnection") -> None:
        self.connection = connection
        self.committed = False
        self.rolled_back = False

    def __enter__(self) -> "FakeConnection":
        return self.connection

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.committed = exc_type is None
        self.rolled_back = exc_type is not None


class FakeConnection:
    def __init__(self, results: list[FakeResult | Exception]) -> None:
        self.results = results
        self.calls: list[dict[str, Any] | None] = []

    def execute(self, statement: object, parameters: dict[str, Any] | None = None) -> FakeResult:
        self.calls.append(parameters)
        next_result = self.results.pop(0)
        if isinstance(next_result, Exception):
            raise next_result
        return next_result


class FakeEngine:
    def __init__(self, results: list[FakeResult | Exception]) -> None:
        self.connection = FakeConnection(results)
        self.transactions: list[FakeTransaction] = []

    def begin(self) -> FakeTransaction:
        transaction = FakeTransaction(self.connection)
        self.transactions.append(transaction)
        return transaction


def clean_records() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "location": "Nairobi",
                "country_code": "KE",
                "latitude": -1.2864,
                "longitude": 36.8172,
                "observed_at": pd.Timestamp("2026-09-18T09:00:00Z"),
                "temperature_c": 25.0,
                "humidity_pct": 60.0,
                "wind_speed_kmh": 10.0,
                "pressure_msl_hpa": 1012.0,
                "weather_code": 2,
                "weather_condition": "Partly cloudy",
                "source": "open-meteo",
                "raw_file_path": "data/raw/weather_nairobi.json",
            },
            {
                "location": "Kisumu",
                "country_code": "KE",
                "latitude": -0.1022,
                "longitude": 34.7617,
                "observed_at": pd.Timestamp("2026-09-18T09:00:00Z"),
                "temperature_c": 27.0,
                "humidity_pct": 55.0,
                "wind_speed_kmh": 8.0,
                "pressure_msl_hpa": 1011.0,
                "weather_code": 0,
                "weather_condition": "Clear sky",
                "source": "open-meteo",
                "raw_file_path": "data/raw/weather_kisumu.json",
            },
        ]
    )


def test_database_url_uses_environment_settings_without_manual_escaping() -> None:
    settings = replace(get_settings(), postgres_password="p@ss word")

    url = build_database_url(settings)

    assert url.drivername == "postgresql+psycopg"
    assert url.password == "p@ss word"
    assert url.database == settings.postgres_db


def test_load_uses_one_transaction_and_skips_duplicate_observations() -> None:
    engine = FakeEngine(
        [FakeResult(scalar=1), FakeResult(rowcount=1), FakeResult(scalar=2), FakeResult(rowcount=0)]
    )
    loader = DatabaseLoader(engine=engine)

    result = loader.load_observations(clean_records(), pipeline_run_id=7)

    assert result.records_loaded == 1
    assert result.duplicates_skipped == 1
    assert engine.transactions[0].committed
    assert len(engine.connection.calls) == 4


def test_load_rolls_back_when_database_execution_fails() -> None:
    engine = FakeEngine([FakeResult(scalar=1), SQLAlchemyError("insert failed")])
    loader = DatabaseLoader(engine=engine)

    with pytest.raises(DatabaseLoadError, match="were not loaded"):
        loader.load_observations(clean_records().iloc[:1], pipeline_run_id=7)

    assert engine.transactions[0].rolled_back


def test_pipeline_run_lifecycle_records_start_and_completion() -> None:
    engine = FakeEngine([FakeResult(scalar=42), FakeResult(rowcount=1)])
    loader = DatabaseLoader(engine=engine)

    run_id = loader.create_pipeline_run()
    loader.complete_pipeline_run(
        run_id,
        records_extracted=4,
        records_loaded=3,
        records_rejected=1,
        status="success",
    )

    assert run_id == 42
    assert all(transaction.committed for transaction in engine.transactions)


def test_failed_run_requires_an_error_message() -> None:
    loader = DatabaseLoader(engine=FakeEngine([]))

    with pytest.raises(ValueError, match="requires an error message"):
        loader.complete_pipeline_run(
            1,
            records_extracted=0,
            records_loaded=0,
            records_rejected=0,
            status="failed",
        )

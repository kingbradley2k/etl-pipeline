"""Unit tests for ETL orchestration with no API or database dependency."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from src.extract import ExtractionResult, FetchedWeatherPayload, Location
from src.load import LoadResult
from src.pipeline import PipelineError, WeatherETLPipeline
from src.transform import TransformationResult


class StubExtractor:
    def __init__(self, result: ExtractionResult | Exception) -> None:
        self.result = result
        self.called = False

    def extract(self) -> ExtractionResult:
        self.called = True
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


class StubLoader:
    def __init__(self, load_result: LoadResult | Exception = LoadResult(1, 0)) -> None:
        self.load_result = load_result
        self.completed: list[dict[str, Any]] = []
        self.loaded_frames: list[pd.DataFrame] = []

    def create_pipeline_run(self) -> int:
        return 42

    def load_observations(self, records: pd.DataFrame, pipeline_run_id: int) -> LoadResult:
        assert pipeline_run_id == 42
        self.loaded_frames.append(records)
        if isinstance(self.load_result, Exception):
            raise self.load_result
        return self.load_result

    def complete_pipeline_run(self, run_id: int, **kwargs: Any) -> None:
        self.completed.append({"run_id": run_id, **kwargs})


def extraction_result() -> ExtractionResult:
    payload = FetchedWeatherPayload(
        location=Location("Nairobi", -1.2864, 36.8172),
        payload={"current": {"time": "2026-09-18T12:00"}},
        raw_file=Path("data/raw/weather_nairobi.json"),
    )
    return ExtractionResult(payloads=[payload])


def transformation_result() -> TransformationResult:
    dataframe = pd.DataFrame([{"location": "Nairobi", "temperature_c": 25.0}])
    return TransformationResult(
        clean_records=dataframe,
        rejected_records=pd.DataFrame([{"reason": "invalid humidity"}]),
        records_received=2,
        records_cleaned=1,
        duplicates_removed=1,
    )


def test_pipeline_orchestrates_successful_run_and_audits_counts() -> None:
    loader = StubLoader(LoadResult(records_loaded=1, duplicates_skipped=2))
    pipeline = WeatherETLPipeline(
        extractor=StubExtractor(extraction_result()),
        loader=loader,
        transformer=lambda _: transformation_result(),
    )

    result = pipeline.run()

    assert result.run_id == 42
    assert result.records_extracted == 1
    assert result.records_loaded == 1
    assert result.records_rejected == 1
    assert result.duplicates_removed == 1
    assert result.duplicates_skipped == 2
    assert loader.completed == [
        {
            "run_id": 42,
            "records_extracted": 1,
            "records_loaded": 1,
            "records_rejected": 1,
            "status": "success",
        }
    ]


def test_pipeline_marks_extraction_failure_with_zero_counts() -> None:
    loader = StubLoader()
    pipeline = WeatherETLPipeline(
        extractor=StubExtractor(RuntimeError("API unavailable")),
        loader=loader,
    )

    with pytest.raises(PipelineError, match="during extraction"):
        pipeline.run()

    assert loader.completed[0]["status"] == "failed"
    assert loader.completed[0]["records_extracted"] == 0
    assert "API unavailable" in loader.completed[0]["error_message"]


def test_pipeline_marks_loading_failure_with_transform_counts() -> None:
    loader = StubLoader(RuntimeError("database unavailable"))
    pipeline = WeatherETLPipeline(
        extractor=StubExtractor(extraction_result()),
        loader=loader,
        transformer=lambda _: transformation_result(),
    )

    with pytest.raises(PipelineError, match="during loading"):
        pipeline.run()

    assert loader.completed[0]["status"] == "failed"
    assert loader.completed[0]["records_extracted"] == 1
    assert loader.completed[0]["records_rejected"] == 1
    assert "database unavailable" in loader.completed[0]["error_message"]

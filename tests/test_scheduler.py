"""Tests for scheduler configuration without starting a background process."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from src.config import get_settings
from src.scheduler import JOB_ID, create_scheduler, run_pipeline_once


class FakeScheduler:
    def __init__(self) -> None:
        self.jobs: list[tuple[object, dict[str, Any]]] = []

    def add_job(self, function: object, **kwargs: Any) -> None:
        self.jobs.append((function, kwargs))


class StubPipeline:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.run_called = False

    def run(self) -> None:
        self.run_called = True
        if self.error:
            raise self.error


def test_create_scheduler_uses_environment_interval_and_overlap_protection() -> None:
    scheduler = FakeScheduler()
    settings = replace(get_settings(), schedule_minutes=60, timezone="Africa/Nairobi")

    returned_scheduler = create_scheduler(settings, scheduler=scheduler)

    assert returned_scheduler is scheduler
    function, options = scheduler.jobs[0]
    assert function is run_pipeline_once
    assert options["trigger"] == "interval"
    assert options["minutes"] == 60
    assert options["id"] == JOB_ID
    assert options["max_instances"] == 1
    assert options["coalesce"] is True
    assert options["misfire_grace_time"] == 300


def test_create_scheduler_rejects_nonpositive_interval() -> None:
    settings = replace(get_settings(), schedule_minutes=0)

    with pytest.raises(ValueError, match="SCHEDULE_MINUTES"):
        create_scheduler(settings, scheduler=FakeScheduler())


def test_scheduled_runner_executes_pipeline_once() -> None:
    pipeline = StubPipeline()

    run_pipeline_once(lambda: pipeline)

    assert pipeline.run_called


def test_scheduled_runner_propagates_pipeline_failure() -> None:
    pipeline = StubPipeline(RuntimeError("pipeline failed"))

    with pytest.raises(RuntimeError, match="pipeline failed"):
        run_pipeline_once(lambda: pipeline)

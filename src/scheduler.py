"""Schedule recurring weather ETL pipeline runs with APScheduler."""

from __future__ import annotations

import logging
from typing import Any, Callable

from apscheduler.schedulers.blocking import BlockingScheduler

from src.config import Settings, get_settings
from src.logging_config import configure_logging
from src.pipeline import WeatherETLPipeline


LOGGER = logging.getLogger(__name__)
JOB_ID = "weather-etl-pipeline"


def run_pipeline_once(
    pipeline_factory: Callable[[], WeatherETLPipeline | Any] = WeatherETLPipeline,
) -> None:
    """Run one ETL job and re-raise failures for APScheduler to report."""
    LOGGER.info("Scheduled pipeline run started")
    try:
        pipeline_factory().run()
    except Exception:
        LOGGER.exception("Scheduled pipeline run failed")
        raise
    LOGGER.info("Scheduled pipeline run completed successfully")


def create_scheduler(
    settings: Settings | None = None,
    scheduler: BlockingScheduler | Any | None = None,
    pipeline_factory: Callable[[], WeatherETLPipeline | Any] = WeatherETLPipeline,
) -> BlockingScheduler | Any:
    """Configure a non-overlapping, interval-based ETL scheduler.

    `max_instances=1` stops one delayed job from overlapping another process in
    the same scheduler. Database uniqueness constraints remain the cross-process
    protection against duplicate observations.
    """
    active_settings = settings or get_settings()
    if active_settings.schedule_minutes <= 0:
        raise ValueError("SCHEDULE_MINUTES must be greater than zero.")

    active_scheduler = scheduler or BlockingScheduler(timezone=active_settings.timezone)
    active_scheduler.add_job(
        run_pipeline_once,
        trigger="interval",
        minutes=active_settings.schedule_minutes,
        id=JOB_ID,
        name="Weather ETL pipeline",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
        kwargs={"pipeline_factory": pipeline_factory},
    )
    LOGGER.info(
        "Scheduler configured: weather ETL runs every %d minutes",
        active_settings.schedule_minutes,
    )
    return active_scheduler


def main() -> None:
    """Start the blocking scheduler until interrupted by the operator."""
    settings = get_settings()
    configure_logging(settings.log_level)
    scheduler = create_scheduler(settings)
    LOGGER.info("Scheduler started; press Ctrl+C to stop")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        LOGGER.info("Scheduler stopped")


if __name__ == "__main__":
    main()

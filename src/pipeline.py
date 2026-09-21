"""Orchestrate weather extraction, transformation, loading, and audit status."""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Callable, Sequence

from src.config import Settings, get_settings
from src.extract import FetchedWeatherPayload, WeatherExtractor
from src.load import DatabaseLoader, LoadResult
from src.logging_config import configure_logging
from src.transform import TransformationResult, transform_payloads


LOGGER = logging.getLogger(__name__)


class PipelineError(RuntimeError):
    """Raised when an ETL stage fails after its failure has been audited."""


@dataclass(frozen=True)
class PipelineResult:
    """Final statistics from one successful ETL pipeline execution."""

    run_id: int
    records_extracted: int
    records_loaded: int
    records_rejected: int
    duplicates_removed: int
    duplicates_skipped: int


class WeatherETLPipeline:
    """Coordinate independently testable ETL stages into one auditable run."""

    def __init__(
        self,
        settings: Settings | None = None,
        extractor: WeatherExtractor | object | None = None,
        loader: DatabaseLoader | object | None = None,
        transformer: Callable[[Sequence[FetchedWeatherPayload]], TransformationResult] = transform_payloads,
    ) -> None:
        self.settings = settings or get_settings()
        self.extractor = extractor or WeatherExtractor(self.settings)
        self.loader = loader or DatabaseLoader(self.settings)
        self.transformer = transformer

    def run(self) -> PipelineResult:
        """Run extract → transform → load and persist final audit statistics."""
        LOGGER.info("Pipeline started")
        try:
            run_id = self.loader.create_pipeline_run()
        except Exception as error:
            LOGGER.exception("Pipeline failed while creating its audit run")
            raise PipelineError("Pipeline failed during audit-run initialization.") from error

        records_extracted = 0
        records_rejected = 0
        records_loaded = 0
        duplicates_removed = 0
        duplicates_skipped = 0
        stage = "extraction"

        try:
            extraction_result = self.extractor.extract()
            records_extracted = extraction_result.records_extracted
            LOGGER.info("Records extracted: %d", records_extracted)

            stage = "transformation"
            transformation_result = self.transformer(extraction_result.payloads)
            records_rejected = transformation_result.records_rejected
            duplicates_removed = transformation_result.duplicates_removed
            LOGGER.info("Duplicates removed: %d", duplicates_removed)
            LOGGER.info("Invalid records rejected: %d", records_rejected)

            stage = "loading"
            load_result: LoadResult = self.loader.load_observations(
                transformation_result.clean_records,
                pipeline_run_id=run_id,
            )
            records_loaded = load_result.records_loaded
            duplicates_skipped = load_result.duplicates_skipped
            LOGGER.info("Records loaded: %d", records_loaded)
            LOGGER.info("Duplicate records skipped by database: %d", duplicates_skipped)

            self.loader.complete_pipeline_run(
                run_id,
                records_extracted=records_extracted,
                records_loaded=records_loaded,
                records_rejected=records_rejected,
                status="success",
            )
        except Exception as error:
            LOGGER.exception("Pipeline failed during %s", stage)
            self._record_failure(
                run_id,
                records_extracted=records_extracted,
                records_loaded=records_loaded,
                records_rejected=records_rejected,
                stage=stage,
                error=error,
            )
            raise PipelineError(f"Pipeline failed during {stage}: {error}") from error

        LOGGER.info("Pipeline completed successfully")
        return PipelineResult(
            run_id=run_id,
            records_extracted=records_extracted,
            records_loaded=records_loaded,
            records_rejected=records_rejected,
            duplicates_removed=duplicates_removed,
            duplicates_skipped=duplicates_skipped,
        )

    def _record_failure(
        self,
        run_id: int,
        *,
        records_extracted: int,
        records_loaded: int,
        records_rejected: int,
        stage: str,
        error: Exception,
    ) -> None:
        """Attempt to audit a failure without masking the original stage error."""
        message = f"{stage} failed: {error}"
        try:
            self.loader.complete_pipeline_run(
                run_id,
                records_extracted=records_extracted,
                records_loaded=records_loaded,
                records_rejected=records_rejected,
                status="failed",
                error_message=message,
            )
        except Exception:
            LOGGER.exception("Could not record failed pipeline run %s", run_id)


def main() -> None:
    """Run the complete ETL pipeline once from the command line."""
    settings = get_settings()
    configure_logging(settings.log_level)
    WeatherETLPipeline(settings).run()


if __name__ == "__main__":
    main()

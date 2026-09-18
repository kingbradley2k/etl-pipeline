"""Environment-backed configuration for the weather ETL application."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class Settings:
    """Application settings loaded from environment variables.

    Secrets are intentionally read at runtime and never stored in source code.
    """

    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    log_level: str
    raw_data_dir: Path
    schedule_minutes: int
    timezone: str
    http_timeout_seconds: int
    http_max_retries: int
    http_retry_backoff_seconds: float


def get_settings() -> Settings:
    """Return the current settings, applying safe development defaults."""
    return Settings(
        postgres_host=os.getenv("POSTGRES_HOST", "localhost"),
        postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
        postgres_db=os.getenv("POSTGRES_DB", "weather_etl"),
        postgres_user=os.getenv("POSTGRES_USER", "weather_user"),
        postgres_password=os.getenv("POSTGRES_PASSWORD", ""),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        raw_data_dir=PROJECT_ROOT / os.getenv("RAW_DATA_DIR", "data/raw"),
        schedule_minutes=int(os.getenv("SCHEDULE_MINUTES", "60")),
        timezone=os.getenv("TIMEZONE", "Africa/Nairobi"),
        http_timeout_seconds=int(os.getenv("HTTP_TIMEOUT_SECONDS", "15")),
        http_max_retries=int(os.getenv("HTTP_MAX_RETRIES", "3")),
        http_retry_backoff_seconds=float(
            os.getenv("HTTP_RETRY_BACKOFF_SECONDS", "1")
        ),
    )

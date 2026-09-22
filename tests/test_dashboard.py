"""Tests for PostgreSQL-dashboard data preparation without Streamlit rendering."""

from __future__ import annotations

import pandas as pd

from dashboard.app import prepare_chart_data


def observations() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "location": "Nairobi",
                "observed_at": "2026-09-18T21:00:00Z",
                "temperature_c": 20.0,
                "humidity_pct": 70.0,
                "weather_condition": "Clear sky",
            },
            {
                "location": "Nairobi",
                "observed_at": "2026-09-18T22:00:00Z",
                "temperature_c": 24.0,
                "humidity_pct": 60.0,
                "weather_condition": None,
            },
            {
                "location": "Kisumu",
                "observed_at": "2026-09-18T22:00:00Z",
                "temperature_c": 28.0,
                "humidity_pct": 50.0,
                "weather_condition": "Clear sky",
            },
        ]
    )


def test_prepare_chart_data_aggregates_required_dashboard_charts() -> None:
    charts = prepare_chart_data(observations())

    assert len(charts.temperature_over_time) == 2
    assert charts.temperature_over_time.iloc[1]["temperature_c"] == 26.0
    assert charts.temperature_by_location.iloc[0]["location"] == "Kisumu"
    assert set(charts.condition_distribution["weather_condition"]) == {"Clear sky", "Unknown"}
    assert charts.daily_temperature_range["local_date"].iloc[0].isoformat() == "2026-09-19"


def test_prepare_chart_data_calculates_daily_temperature_range_per_location() -> None:
    charts = prepare_chart_data(observations())
    nairobi = charts.daily_temperature_range.query("location == 'Nairobi'").iloc[0]

    assert nairobi["minimum_temperature_c"] == 20.0
    assert nairobi["maximum_temperature_c"] == 24.0
    assert nairobi["temperature_range_c"] == 4.0

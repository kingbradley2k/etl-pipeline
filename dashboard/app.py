"""Interactive PostgreSQL-backed dashboard for Kenyan weather observations."""

from __future__ import annotations

from dataclasses import dataclass
import logging
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import streamlit as st

from src.config import get_settings
from src.load import build_database_url


LOGGER = logging.getLogger(__name__)

LOCATIONS_QUERY = text(
    """
    SELECT city
    FROM locations
    ORDER BY city
    """
)

OBSERVATIONS_QUERY = text(
    """
    SELECT
        location.city AS location,
        observation.observed_at,
        observation.temperature_c,
        observation.humidity_pct,
        observation.wind_speed_kmh,
        observation.pressure_msl_hpa,
        observation.weather_condition
    FROM weather_observations AS observation
    JOIN locations AS location ON location.location_id = observation.location_id
    WHERE (:location = 'All' OR location.city = :location)
        AND observation.observed_at >= :start_date
        AND observation.observed_at < :end_date + INTERVAL '1 day'
    ORDER BY observation.observed_at, location.city
    """
)

LATEST_RUN_QUERY = text(
    """
    SELECT
        run_id,
        status,
        started_at,
        completed_at,
        records_extracted,
        records_loaded,
        records_rejected,
        error_message
    FROM pipeline_runs
    ORDER BY started_at DESC
    LIMIT 1
    """
)

LAST_SUCCESSFUL_RUN_QUERY = text(
    """
    SELECT
        run_id,
        completed_at,
        records_loaded,
        records_rejected
    FROM pipeline_runs
    WHERE status = 'success'
    ORDER BY completed_at DESC
    LIMIT 1
    """
)


@dataclass(frozen=True)
class ChartData:
    """Dataframes pre-aggregated for the dashboard's five required charts."""

    temperature_over_time: pd.DataFrame
    humidity_over_time: pd.DataFrame
    temperature_by_location: pd.DataFrame
    condition_distribution: pd.DataFrame
    daily_temperature_range: pd.DataFrame


@st.cache_resource
def get_engine() -> Engine:
    """Create one reusable SQLAlchemy engine from the local environment."""
    settings = get_settings()
    return create_engine(build_database_url(settings), pool_pre_ping=True)


def fetch_dataframe(engine: Engine, query: object, params: dict[str, object] | None = None) -> pd.DataFrame:
    """Run a parameterized SQL query and return a dataframe."""
    with engine.connect() as connection:
        return pd.read_sql(query, connection, params=params)


def prepare_chart_data(observations: pd.DataFrame) -> ChartData:
    """Aggregate filtered database records for Streamlit's chart components."""
    data = observations.copy()
    data["observed_at"] = pd.to_datetime(data["observed_at"], utc=True)
    data["local_date"] = data["observed_at"].dt.tz_convert("Africa/Nairobi").dt.date

    temperature_over_time = (
        data.groupby("observed_at", as_index=False)["temperature_c"].mean()
        .sort_values("observed_at")
    )
    humidity_over_time = (
        data.groupby("observed_at", as_index=False)["humidity_pct"].mean()
        .sort_values("observed_at")
    )
    temperature_by_location = (
        data.groupby("location", as_index=False)["temperature_c"].mean()
        .sort_values("temperature_c", ascending=False)
    )
    condition_distribution = (
        data.assign(weather_condition=data["weather_condition"].fillna("Unknown"))
        .groupby("weather_condition", as_index=False)
        .size()
        .rename(columns={"size": "observation_count"})
        .sort_values("observation_count", ascending=False)
    )
    daily_temperature_range = (
        data.groupby(["local_date", "location"], as_index=False)["temperature_c"]
        .agg(minimum_temperature_c="min", maximum_temperature_c="max")
    )
    daily_temperature_range["temperature_range_c"] = (
        daily_temperature_range["maximum_temperature_c"]
        - daily_temperature_range["minimum_temperature_c"]
    )
    daily_temperature_range = daily_temperature_range.sort_values(["local_date", "location"])

    return ChartData(
        temperature_over_time=temperature_over_time,
        humidity_over_time=humidity_over_time,
        temperature_by_location=temperature_by_location,
        condition_distribution=condition_distribution,
        daily_temperature_range=daily_temperature_range,
    )


def render_dashboard() -> None:
    """Render the complete interactive dashboard from PostgreSQL query results."""
    st.set_page_config(page_title="Kenyan Weather ETL", page_icon="🌦️", layout="wide")
    st.title("Kenyan Weather ETL Dashboard")
    st.caption("Data is queried from PostgreSQL; raw JSON files are never used here.")

    try:
        engine = get_engine()
        locations = fetch_dataframe(engine, LOCATIONS_QUERY)["city"].tolist()
    except (SQLAlchemyError, KeyError) as error:
        LOGGER.exception("Dashboard could not load PostgreSQL locations")
        st.error(f"Could not connect to PostgreSQL: {error}")
        st.stop()

    with st.sidebar:
        st.header("Filters")
        selected_location = st.selectbox("Location", ["All", *locations])
        default_end = date.today()
        default_start = default_end - timedelta(days=30)
        selected_dates = st.date_input(
            "Observation date range",
            value=(default_start, default_end),
            max_value=default_end,
        )

    if not isinstance(selected_dates, tuple) or len(selected_dates) != 2:
        st.info("Select both a start and end date to view observations.")
        st.stop()
    start_date, end_date = selected_dates
    if start_date > end_date:
        st.error("The start date must be on or before the end date.")
        st.stop()

    try:
        observations = fetch_dataframe(
            engine,
            OBSERVATIONS_QUERY,
            {
                "location": selected_location,
                "start_date": start_date,
                "end_date": end_date,
            },
        )
        latest_run = fetch_dataframe(engine, LATEST_RUN_QUERY)
        last_successful_run = fetch_dataframe(engine, LAST_SUCCESSFUL_RUN_QUERY)
    except SQLAlchemyError as error:
        LOGGER.exception("Dashboard could not query PostgreSQL observations")
        st.error(f"Could not retrieve dashboard data: {error}")
        st.stop()

    _render_pipeline_status(latest_run, last_successful_run)
    if observations.empty:
        st.warning("No weather observations match the selected filters.")
        return

    _render_kpis(observations, latest_run)
    charts = prepare_chart_data(observations)
    _render_charts(charts)


def _render_pipeline_status(latest_run: pd.DataFrame, last_successful_run: pd.DataFrame) -> None:
    """Display current pipeline health and the latest successful load counts."""
    st.subheader("Pipeline health")
    left, right = st.columns(2)
    with left:
        if latest_run.empty:
            st.info("No pipeline runs have been recorded yet.")
        else:
            run = latest_run.iloc[0]
            completed = run["completed_at"] or "still running"
            st.write(f"Latest run #{run['run_id']}: **{run['status']}** ({completed})")
            if run["error_message"]:
                st.error(run["error_message"])
    with right:
        if last_successful_run.empty:
            st.info("No successful pipeline run has been recorded yet.")
        else:
            run = last_successful_run.iloc[0]
            st.write(f"Last successful run: **{run['completed_at']}**")
            st.write(
                f"Loaded: **{run['records_loaded']}** · "
                f"Rejected: **{run['records_rejected']}**"
            )


def _render_kpis(observations: pd.DataFrame, latest_run: pd.DataFrame) -> None:
    """Display core weather and pipeline metrics for the active filter."""
    latest_status = "No runs" if latest_run.empty else str(latest_run.iloc[0]["status"])
    columns = st.columns(6)
    columns[0].metric("Average temperature", f"{observations['temperature_c'].mean():.1f} °C")
    columns[1].metric("Maximum temperature", f"{observations['temperature_c'].max():.1f} °C")
    columns[2].metric("Minimum temperature", f"{observations['temperature_c'].min():.1f} °C")
    columns[3].metric("Average humidity", f"{observations['humidity_pct'].mean():.1f}%")
    columns[4].metric("Total observations", f"{len(observations):,}")
    columns[5].metric("Latest pipeline run", latest_status.title())


def _render_charts(charts: ChartData) -> None:
    """Render the dashboard's weather trend, comparison, and distribution charts."""
    st.subheader("Weather analysis")
    first_row, second_row = st.columns(2)
    with first_row:
        st.markdown("#### Temperature over time")
        st.line_chart(charts.temperature_over_time, x="observed_at", y="temperature_c")
        st.markdown("#### Temperature by location")
        st.bar_chart(charts.temperature_by_location, x="location", y="temperature_c")
        st.markdown("#### Daily temperature range")
        st.bar_chart(
            charts.daily_temperature_range,
            x="local_date",
            y="temperature_range_c",
            color="location",
        )
    with second_row:
        st.markdown("#### Humidity over time")
        st.line_chart(charts.humidity_over_time, x="observed_at", y="humidity_pct")
        st.markdown("#### Weather-condition distribution")
        st.bar_chart(charts.condition_distribution, x="weather_condition", y="observation_count")


if __name__ == "__main__":
    render_dashboard()

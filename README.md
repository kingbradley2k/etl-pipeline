# Kenyan Weather ETL Pipeline

## Project overview

This portfolio project will collect model-derived current weather conditions for
Kakamega, Kisumu, Nairobi, and Mombasa; archive each original API response;
validate and transform the data; store it in PostgreSQL; and present analysis in
a Streamlit dashboard.

## Architecture

```mermaid
flowchart LR
    A[Open-Meteo API] --> B[Extract: requests]
    B --> C[Raw JSON archive\ndata/raw]
    B --> D[Transform and validate\npandas]
    D -->|valid records| E[(PostgreSQL)]
    D -->|rejections| F[Application logs]
    E --> G[Analytical SQL]
    E --> H[Streamlit dashboard]
    I[APScheduler] --> J[Pipeline orchestrator]
    J --> B
    J --> D
    J --> E
    E --> K[pipeline_runs audit table]
```

## Data source

The project will use the [Open-Meteo Weather Forecast API](https://open-meteo.com/en/docs).
It returns model-derived current weather conditions for supplied coordinates.
The planned fields are timestamp, temperature, relative humidity, wind speed,
mean sea-level pressure, weather code, latitude, and longitude.

## Design decisions

- Each city is requested by coordinate, avoiding ambiguous city-name lookups.
- Original API payloads are retained before transformation for traceability.
- PostgreSQL will enforce a unique `(location_id, observed_at, source)` key to
  prevent duplicate scheduled observations.
- Invalid records will be rejected explicitly and counted in pipeline logs.
- Runtime configuration is supplied through `.env`; secrets are excluded from Git.

## Planned database model

| Table | Purpose |
| --- | --- |
| `locations` | Kenyan city metadata and coordinates |
| `weather_observations` | Validated weather records keyed by location and timestamp |
| `pipeline_runs` | Audit history, status, counts, and failures |

## Development status

Phase 2 implements extraction with bounded HTTP retries, request timeouts,
per-city logging, and raw JSON archival. Transformation, loading, scheduling,
analytics, dashboard functionality, Docker services, and full tests will be
introduced in their respective later phases.

## Run Phase 2 extraction

Create a local `.env` from `.env.example`, activate the virtual environment, and
run the extraction module:

```powershell
Copy-Item .env.example .env
.\.venv\Scripts\Activate.ps1
python -m src.extract
```

Each successful city request creates an ignored file named like
`data/raw/weather_nairobi_2026-09-18T120000Z.json`. Test without contacting the
API with:

```powershell
pytest tests/test_extract.py -q
```

If your organization intercepts HTTPS traffic, configure its trusted CA bundle
in the operating-system certificate store or set `REQUESTS_CA_BUNDLE` to the
approved CA file. Do not disable TLS verification.

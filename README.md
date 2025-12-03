# Airflow + DuckDB Air Quality Pipeline

Small, self-contained project that fetches hourly air-quality data from Open-Meteo, keeps the raw JSON, enriches it, and loads it into DuckDB via an Apache Airflow DAG running in Docker. It’s built to be simple, repeatable, and to demonstrate good habits like idempotent DAG runs, data quality checks, and unit tests.

## What it does
- Uses Airflow (Celery executor) via Docker Compose to orchestrate the daily `elt_air_quality` DAG (`dags/elt_air_quality/air_quality.py`).
- Fetches hourly PM10, PM2.5, and UV index readings from Open-Meteo for Paris, stores the raw payload on disk, and reloads it for reproducibility.
- Enriches each row with latitude, longitude, timezone, city, logical date, and the DAG execution timestamp (`dags/elt_air_quality/tools/utils.py`).
- Upserts the result into DuckDB at `data/database/air_quality_db.db` in table `raw.air_quality_raw`.
- Keeps raw snapshots under `data/raw/air_quality/` so you can replay or backfill without refetching.

## Stack
- Airflow 3.1 (Celery executor, API server) + Redis + Postgres via `docker-compose.yaml`
- DuckDB (file-backed)
- Python: pandas, requests, duckdb (used inside tasks)

## Repo tour
- `docker-compose.yaml`: brings up Airflow (Celery + API), Redis, Postgres, and wiring for local development.
- `dags/elt_air_quality/air_quality.py`: the DAG definition and task wiring; tasks are idempotent so re-running a day yields the same result.
- `dags/elt_air_quality/tools/utils.py`: fetch, persist, enrich, and load helpers (shared between DAG and tests).
- `dags/elt_air_quality/tools/config.py`: paths, Open-Meteo defaults, and table names.
- `data/raw/air_quality/`: saved API responses (ignored by git).
- `data/database/air_quality_db.db`: DuckDB file with `raw.air_quality_raw` (ignored by git).

## Data quality
The DAG ends with a `data_quality` task group that fails the run if any check trips:
- Completeness: each date must have 24 hourly rows and 24 distinct timestamps.
- Not-null: no NULLs in pm10, pm2_5, uv_index, latitude, longitude, timezone, city.
- Range bounds: pm10 and pm2_5 in [0, 500], uv_index in [0, 15].

## Running locally
Prereqs: Docker Desktop (or compatible) and `docker compose`.

1) Set Airflow UID (prevents root-owned files):
   ```bash
   export AIRFLOW_UID=$(id -u)
   export AIRFLOW_PROJ_DIR=.
   ```
   You can place these in `.env` so Docker Compose picks them up automatically.

2) One-time initialization (creates metadata DB, user, volumes):
   ```bash
   docker compose up airflow-init
   ```

3) Start the stack:
   ```bash
   docker compose up
   ```
   - Airflow webserver/API: http://localhost:8080 (default user/pass: `airflow` / `airflow`)
   - Scheduler, workers, triggerer, Redis, and Postgres start together.


4) Optional: 
   - Use plot.ipynb to plot PM2.5/PM10 data loaded in the database.


## Clean up
Stop everything and remove containers (keeps volumes):
```bash
docker compose down

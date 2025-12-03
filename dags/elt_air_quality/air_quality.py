from __future__ import annotations
from datetime import datetime
from pathlib import Path
from typing import Any
import duckdb
import pandas as pd
from airflow.sdk import dag, task
from airflow.decorators import task_group

from elt_air_quality.tools.config import (
    AIR_QUALITY_DB_PATH,
    air_quality_filename_for_date,
    BASE_URL,
    DATABASE_DIR,
    DEFAULT_AIR_QUALITY_PARAMS,
    RAW_DATA_DIR,
    SCHEMA,
    TABLE,
    VIEW_NAME,
)
from elt_air_quality.tools.utils import (
    enrich_raw_data,
    ensure_air_quality_table,
    fetch_air_quality_payload,
    load_dataframe,
    load_payload_from_disk,
    persist_payload as persist_payload_file,
)


@dag(
    dag_id="elt_air_quality",
    start_date=datetime(2025, 11, 12),
    schedule="@daily",
    catchup=False,
    tags=["air_quality", "duckdb"],
)
def elt_air_quality():
    @task
    def fetch_payload(ds: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        request_params = params or dict(DEFAULT_AIR_QUALITY_PARAMS)
        request_params["start_date"] = ds
        request_params["end_date"] = ds
        return fetch_air_quality_payload(BASE_URL, request_params, timeout=60)

    @task
    def persist_payload(payload: dict[str, Any], ds: str) -> str:
        path = persist_payload_file(payload, RAW_DATA_DIR, air_quality_filename_for_date(ds))
        return str(path)

    @task
    def load_from_disk(path_str: str) -> dict[str, Any]:
        return load_payload_from_disk(Path(path_str))

    @task
    def enrich(payload: dict[str, Any]) -> list[dict[str, Any]]:
        dataframe = enrich_raw_data(payload)
        safe_df = dataframe.assign(
            time=dataframe["time"].dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            date=dataframe["date"].astype(str),
            execution_time=dataframe["execution_time"].dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
        return safe_df.to_dict(orient="records")

    @task
    def ensure_table() -> None:
        DATABASE_DIR.mkdir(parents=True, exist_ok=True)
        ensure_air_quality_table(AIR_QUALITY_DB_PATH, SCHEMA, TABLE)

    @task
    def load(records: list[dict[str, Any]]) -> int:
        dataframe = pd.DataFrame.from_records(records)
        dataframe["time"] = pd.to_datetime(dataframe["time"], utc=True)
        dataframe["execution_time"] = pd.to_datetime(dataframe["execution_time"], utc=True)
        dataframe["date"] = pd.to_datetime(dataframe["date"]).dt.date

        return load_dataframe(
            AIR_QUALITY_DB_PATH,
            SCHEMA,
            TABLE,
            VIEW_NAME,
            dataframe,
        )

    @task
    def check_completeness() -> None:
        """Ensure each date has 24 hourly rows and distinct timestamps."""
        query = """
        SELECT date, COUNT(*) AS rows_count, COUNT(DISTINCT time) AS distinct_times
        FROM {schema}.{table}
        GROUP BY 1
        HAVING rows_count != 24 OR distinct_times != 24
        """.format(schema=SCHEMA, table=TABLE)
        with duckdb.connect(database=str(AIR_QUALITY_DB_PATH)) as con:
            failures = con.sql(query).fetchall()
        if failures:
            raise ValueError(f"Completeness failure for dates: {failures}")

    @task
    def check_not_nulls() -> None:
        """Ensure critical columns have no NULLs."""
        critical_cols = ["pm10", "pm2_5", "uv_index", "latitude", "longitude", "timezone", "city"]
        conditions = " OR ".join(f"{col} IS NULL" for col in critical_cols)
        query = f"SELECT COUNT(*) FROM {SCHEMA}.{TABLE} WHERE {conditions}"
        with duckdb.connect(database=str(AIR_QUALITY_DB_PATH)) as con:
            count = con.sql(query).fetchone()[0]
        if count > 0:
            raise ValueError(f"Nulls detected in critical columns count={count}")

    @task
    def check_ranges() -> None:
        """Validate measure ranges in a single connection to reduce contention."""
        queries = {
            "pm10": f"SELECT COUNT(*) FROM {SCHEMA}.{TABLE} WHERE pm10 < 0 OR pm10 > 500",
            "pm2_5": f"SELECT COUNT(*) FROM {SCHEMA}.{TABLE} WHERE pm2_5 < 0 OR pm2_5 > 500",
            "uv_index": f"SELECT COUNT(*) FROM {SCHEMA}.{TABLE} WHERE uv_index < 0 OR uv_index > 15",
        }
        with duckdb.connect(database=str(AIR_QUALITY_DB_PATH)) as con:
            results = {name: con.sql(q).fetchone()[0] for name, q in queries.items()}
        failures = {k: v for k, v in results.items() if v > 0}
        if failures:
            raise ValueError(f"Range check failures: {failures}")

    @task_group(group_id="data_quality")
    def data_quality_checks():
        completeness = check_completeness()
        not_nulls = check_not_nulls()
        ranges = check_ranges()
        completeness >> not_nulls >> ranges

    raw_payload = fetch_payload()
    persisted_path = persist_payload(raw_payload)
    reloaded_payload = load_from_disk(persisted_path)
    enriched_records = enrich(reloaded_payload)
    created = ensure_table()
    inserted = load(enriched_records)
    dq = data_quality_checks()
    created >> inserted >> dq


dag = elt_air_quality()

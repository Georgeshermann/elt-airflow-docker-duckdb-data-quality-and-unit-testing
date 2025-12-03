import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import duckdb
import pandas as pd
import requests

from elt_air_quality.tools.schemas import COLUMN_ORDER

CITY = "Paris"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", force=True)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def fetch_air_quality_payload(
    url: str,
    params: Mapping[str, Any],
    *,
    timeout: int = 30,
) -> dict[str, Any]:
    """Request the air-quality payload and return the parsed JSON."""
    logger.info("Fetching air quality payload url=%s params=%s timeout=%s", url, params, timeout)
    response = requests.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    logger.info("Received response status=%s", response.status_code)
    return response.json()


def persist_payload(payload: Mapping[str, Any], output_dir: Path, filename: str) -> Path:
    """Write the raw payload to disk so it can be reprocessed later."""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / filename
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    logger.info("Persisted raw payload to %s", output_path)
    return output_path


def load_payload_from_disk(path: Path) -> dict[str, Any]:
    """Reload a previously stored payload."""
    logger.info("Loading payload from %s", path)
    return json.loads(path.read_text(encoding="utf-8"))


def enrich_raw_data(payload: Mapping[str, Any], columns: Iterable[str] = COLUMN_ORDER) -> pd.DataFrame:
    """Convert the hourly payload into a dataframe enriched with derived fields."""
    execution_ts = datetime.now(timezone.utc)
    dataframe = pd.DataFrame(payload["hourly"])
    dataframe["time"] = pd.to_datetime(dataframe["time"], utc=True)
    dataframe["date"] = dataframe["time"].dt.date
    dataframe["latitude"] = payload.get("latitude")
    dataframe["longitude"] = payload.get("longitude")
    dataframe["timezone"] = payload.get("timezone")
    dataframe["city"] = CITY
    dataframe["execution_time"] = execution_ts
    enriched = dataframe[list(columns)].drop_duplicates(subset=["time"])
    logger.info("Enriched dataframe rows=%s columns=%s", len(enriched), list(columns))
    return enriched


def ensure_air_quality_table(db_path: Path, schema: str, table: str) -> None:
    """Create the destination schema/table if they do not already exist."""
    logger.info("Ensuring table exists db=%s schema=%s table=%s", db_path, schema, table)
    with duckdb.connect(database=str(db_path)) as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                time TIMESTAMPTZ PRIMARY KEY,
                pm10 DOUBLE,
                pm2_5 DOUBLE,
                uv_index DOUBLE,
                latitude DOUBLE,
                longitude DOUBLE,
                timezone VARCHAR,
                city VARCHAR,
                date DATE,
                execution_time TIMESTAMP
            )
            """
        )


def load_dataframe(
    db_path: Path,
    schema: str,
    table: str,
    view_name: str,
    dataframe: pd.DataFrame,
    columns: Iterable[str] = COLUMN_ORDER,
) -> int:
    """Upsert the enriched dataframe into DuckDB."""
    df_to_load = dataframe[list(columns)].copy()

    with duckdb.connect(database=str(db_path)) as con:
        con.register(view_name, df_to_load)
        con.execute(f"INSERT OR REPLACE INTO {schema}.{table} SELECT * FROM {view_name}")
        row_count = con.sql(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()[0]

    logger.info("Loaded dataframe into %s.%s row_count=%s", schema, table, row_count)
    return row_count

"""Simple configuration constants for the air quality pipeline."""

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = BASE_DIR / "data"

SOURCE = "air_quality"
RAW_DATA_DIR = DATA_DIR / "raw" / SOURCE
DATABASE_DIR = DATA_DIR / "database"

def air_quality_filename_for_date(ds: str) -> str:
    """Return a predictable filename for the given logical date."""
    return f"{SOURCE}_{ds}.json"


AIR_QUALITY_DB_PATH = DATABASE_DIR / "air_quality_db.db"
SCHEMA = "raw"
TABLE = "air_quality_raw"
VIEW_NAME = "air_quality_df"

BASE_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
DEFAULT_AIR_QUALITY_PARAMS = {
    "latitude": 52.52,
    "longitude": 13.41,
    "hourly": "pm10,pm2_5,uv_index",
    "timezone": "UTC",
}

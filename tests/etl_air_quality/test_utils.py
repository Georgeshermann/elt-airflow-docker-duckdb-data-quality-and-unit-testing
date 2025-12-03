import pandas as pd

from elt_air_quality.tools.utils import enrich_raw_data, CITY


def test_enrich_raw_data_adds_metadata_and_deduplicates():
    payload = {
        "hourly": {
            "time": ["2025-11-04T00:00", "2025-11-04T00:00", "2025-11-04T01:00"],
            "pm10": [10.0, 10.0, 12.0],
            "pm2_5": [5.0, 5.0, 6.0],
            "uv_index": [0.0, 0.0, 0.1],
        },
        "latitude": 48.8566,
        "longitude": 2.3522,
        "timezone": "Europe/Paris",
    }

    df = enrich_raw_data(payload)

    assert list(df.columns) == [
        "time",
        "pm10",
        "pm2_5",
        "uv_index",
        "latitude",
        "longitude",
        "timezone",
        "city",
        "date",
        "execution_time",
    ]
    # Duplicated hourly rows are dropped
    assert len(df) == 2
    assert pd.api.types.is_datetime64tz_dtype(df["time"])
    assert pd.api.types.is_datetime64_any_dtype(df["execution_time"])
    assert df["latitude"].iloc[0] == payload["latitude"]
    assert df["longitude"].iloc[0] == payload["longitude"]
    assert df["timezone"].iloc[0] == payload["timezone"]
    assert df["city"].iloc[0] == CITY

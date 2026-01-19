from __future__ import annotations

import pandas as pd

from metrobikeatlas.ingestion.open_meteo import normalize_open_meteo_hourly


def test_normalize_open_meteo_hourly_produces_expected_schema() -> None:
    payload = {
        "hourly": {
            "time": ["2026-01-01T00:00", "2026-01-01T01:00"],
            "temperature_2m": [18.2, 18.0],
            "precipitation": [0.0, 1.2],
            "relative_humidity_2m": [70, 72],
        }
    }
    df = normalize_open_meteo_hourly(payload, city="Taipei")
    assert list(df.columns) == ["ts", "city", "temp_c", "precip_mm", "humidity_pct"]
    assert len(df) == 2
    assert set(df["city"].unique()) == {"Taipei"}
    assert df["ts"].iloc[0].endswith("Z")

    # Ensure dtypes are numeric (or nullable) for metrics.
    assert pd.api.types.is_numeric_dtype(df["temp_c"])
    assert pd.api.types.is_numeric_dtype(df["precip_mm"])
    assert pd.api.types.is_numeric_dtype(df["humidity_pct"])


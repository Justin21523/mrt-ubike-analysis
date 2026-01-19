from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from metrobikeatlas.ingestion.external_inputs import (
    load_external_metro_stations_csv,
    validate_external_metro_stations_df,
)


def test_load_external_metro_stations_csv_normalizes_columns(tmp_path: Path) -> None:
    p = tmp_path / "metro.csv"
    p.write_text("station_id,name,lat,lon\nA,Alpha,25.0,121.0\n", encoding="utf-8")
    df = load_external_metro_stations_csv(p)
    assert list(df.columns) == ["station_id", "name", "lat", "lon", "city", "system"]
    assert df.loc[0, "city"] == "External"
    assert df.loc[0, "system"] == "EXTERNAL"


def test_validate_external_metro_stations_df_catches_errors() -> None:
    df = pd.DataFrame([{"station_id": "", "name": "x", "lat": 999, "lon": 0}])
    issues = validate_external_metro_stations_df(df)
    assert any(i.level == "error" for i in issues)


def test_load_external_metro_stations_csv_requires_columns(tmp_path: Path) -> None:
    p = tmp_path / "metro.csv"
    p.write_text("id,name,lat,lon\nA,Alpha,25.0,121.0\n", encoding="utf-8")
    with pytest.raises(ValueError):
        load_external_metro_stations_csv(p)


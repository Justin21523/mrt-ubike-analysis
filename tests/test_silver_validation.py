from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from metrobikeatlas.quality.silver import validate_silver_dir


def _write_min_silver(dir_path: Path) -> None:
    dir_path.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {"station_id": "M1", "name": "Metro 1", "lat": 25.0, "lon": 121.0, "city": "Taipei", "system": "TRTC"},
        ]
    ).to_csv(dir_path / "metro_stations.csv", index=False)

    pd.DataFrame(
        [
            {"station_id": "B1", "name": "Bike 1", "lat": 25.0005, "lon": 121.0, "city": "Taipei", "capacity": 40},
        ]
    ).to_csv(dir_path / "bike_stations.csv", index=False)

    pd.DataFrame(
        [
            {"metro_station_id": "M1", "bike_station_id": "B1", "distance_m": 80.0},
        ]
    ).to_csv(dir_path / "metro_bike_links.csv", index=False)


def test_validate_silver_ok(tmp_path: Path) -> None:
    _write_min_silver(tmp_path)
    issues = validate_silver_dir(tmp_path, strict=True)
    assert not [i for i in issues if i.level == "error"]


def test_validate_silver_missing_required_file_raises(tmp_path: Path) -> None:
    (tmp_path / "metro_stations.csv").write_text("station_id,name,lat,lon,city,system\n", encoding="utf-8")
    with pytest.raises(ValueError):
        validate_silver_dir(tmp_path, strict=True)


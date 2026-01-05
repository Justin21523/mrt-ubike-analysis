from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

import pandas as pd

from metrobikeatlas.config.models import AppConfig
from metrobikeatlas.preprocessing.temporal_align import align_timeseries


class LocalRepository:
    """
    Read preprocessed Silver tables from `data/silver/`.

    Expected files (CSV):
    - metro_stations.csv
    - bike_stations.csv
    - metro_bike_links.csv
    - bike_timeseries.csv
    - metro_timeseries.csv (optional)
    """

    def __init__(self, config: AppConfig, *, silver_dir: Path = Path("data/silver")) -> None:
        self._config = config
        self._silver_dir = silver_dir

        self._metro_stations = self._read_required_csv("metro_stations.csv")
        self._bike_stations = self._read_required_csv("bike_stations.csv")
        self._links = self._read_required_csv("metro_bike_links.csv")
        self._bike_ts = self._read_required_csv("bike_timeseries.csv", parse_dates=["ts"])
        self._metro_ts = self._read_optional_csv("metro_timeseries.csv", parse_dates=["ts"])

    def list_metro_stations(self) -> list[dict[str, Any]]:
        cols = ["station_id", "name", "lat", "lon", "city", "system"]
        df = self._metro_stations[cols].copy()
        return df.to_dict(orient="records")

    def nearby_bike(self, metro_station_id: str) -> list[dict[str, Any]]:
        links = self._links[self._links["metro_station_id"] == metro_station_id].copy()
        if links.empty:
            raise KeyError(metro_station_id)

        bikes = self._bike_stations.copy()
        merged = links.merge(bikes, left_on="bike_station_id", right_on="station_id", how="left")
        merged = merged.sort_values("distance_m")

        out = []
        for _, row in merged.iterrows():
            out.append(
                {
                    "station_id": str(row["station_id"]),
                    "name": str(row["name"]),
                    "lat": float(row["lat"]),
                    "lon": float(row["lon"]),
                    "capacity": None if pd.isna(row.get("capacity")) else int(row.get("capacity")),
                    "distance_m": float(row["distance_m"]),
                }
            )
        return out

    def station_timeseries(self, metro_station_id: str) -> dict[str, Any]:
        # Metro series (optional)
        metro_points: list[dict[str, Any]] = []
        if self._metro_ts is not None and not self._metro_ts.empty:
            metro_df = self._metro_ts[self._metro_ts["station_id"] == metro_station_id].copy()
            if not metro_df.empty:
                metro_points = [
                    {"ts": row["ts"], "value": float(row["value"])}
                    for _, row in metro_df.sort_values("ts").iterrows()
                ]

        # Bike series (aggregate nearby bike stations)
        links = self._links[self._links["metro_station_id"] == metro_station_id].copy()
        if links.empty:
            raise KeyError(metro_station_id)
        bike_ids = set(links["bike_station_id"].astype(str).tolist())

        bike_df = self._bike_ts[self._bike_ts["station_id"].astype(str).isin(bike_ids)].copy()
        if bike_df.empty:
            bike_points: list[dict[str, Any]] = []
        else:
            aligned = align_timeseries(
                bike_df,
                ts_col="ts",
                group_cols=(),
                value_cols=("available_bikes",),
                granularity=self._config.temporal.granularity,
                timezone=self._config.temporal.timezone,
                agg="sum",
            )
            aligned = aligned.sort_values("ts")
            bike_points = [
                {"ts": row["ts"], "value": float(row["available_bikes"])}
                for _, row in aligned.iterrows()
            ]

        return {
            "station_id": metro_station_id,
            "granularity": self._config.temporal.granularity,
            "timezone": self._config.temporal.timezone,
            "series": [
                {"metric": "metro_ridership", "points": metro_points},
                {"metric": "bike_available_bikes", "points": bike_points},
            ],
        }

    def _read_required_csv(self, filename: str, **kwargs) -> pd.DataFrame:
        path = self._silver_dir / filename
        if not path.exists():
            raise FileNotFoundError(f"Missing required Silver file: {path}")
        return pd.read_csv(path, **kwargs)

    def _read_optional_csv(self, filename: str, **kwargs) -> pd.DataFrame | None:
        path = self._silver_dir / filename
        if not path.exists():
            return None
        return pd.read_csv(path, **kwargs)


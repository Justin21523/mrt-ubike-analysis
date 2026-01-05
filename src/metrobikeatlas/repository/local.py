from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from metrobikeatlas.analytics.similarity import find_similar_stations
from metrobikeatlas.config.models import AppConfig
from metrobikeatlas.preprocessing.temporal_align import align_timeseries, compute_rent_return_proxy


class LocalRepository:
    """
    Read preprocessed Silver tables from `data/silver/`.

    Expected files (CSV):
    - metro_stations.csv
    - bike_stations.csv
    - metro_bike_links.csv
    - bike_timeseries.csv
    - metro_timeseries.csv (optional; if missing, API falls back to a bike-derived proxy)
    """

    def __init__(self, config: AppConfig, *, silver_dir: Path = Path("data/silver")) -> None:
        self._config = config
        self._silver_dir = silver_dir

        self._metro_stations = self._read_required_csv("metro_stations.csv")
        self._bike_stations = self._read_required_csv("bike_stations.csv")
        self._links = self._read_required_csv("metro_bike_links.csv")
        self._bike_ts = self._read_required_csv("bike_timeseries.csv", parse_dates=["ts"])
        self._metro_ts = self._read_optional_csv("metro_timeseries.csv", parse_dates=["ts"])

        self._station_features = self._read_optional_path(config.features.station_features_path)
        self._station_clusters = self._read_optional_path(
            config.features.station_features_path.parent / "station_clusters.csv"
        )
        self._feature_correlations = self._read_optional_path(
            config.features.station_features_path.parent / "feature_correlations.csv"
        )
        self._regression_coefficients = self._read_optional_path(
            config.features.station_features_path.parent / "regression_coefficients.csv"
        )

    def list_metro_stations(self) -> list[dict[str, Any]]:
        cols = ["station_id", "name", "lat", "lon", "city", "system"]
        df = self._metro_stations[cols].copy()

        if self._station_features is not None and "district" in self._station_features.columns:
            district_df = self._station_features[["station_id", "district"]].copy()
            district_df["station_id"] = district_df["station_id"].astype(str)
            df["station_id"] = df["station_id"].astype(str)
            df = df.merge(district_df, on="station_id", how="left")
        else:
            df["district"] = None

        if self._station_clusters is not None and {"station_id", "cluster"} <= set(self._station_clusters.columns):
            cluster_df = self._station_clusters[["station_id", "cluster"]].copy()
            cluster_df["station_id"] = cluster_df["station_id"].astype(str)
            df["station_id"] = df["station_id"].astype(str)
            df = df.merge(cluster_df, on="station_id", how="left")
        else:
            df["cluster"] = None

        return df.to_dict(orient="records")

    def list_bike_stations(self) -> list[dict[str, Any]]:
        cols = ["station_id", "name", "lat", "lon", "city", "operator", "capacity"]
        present = [c for c in cols if c in self._bike_stations.columns]
        df = self._bike_stations[present].copy()
        if "station_id" in df.columns:
            df["station_id"] = df["station_id"].astype(str)
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
        # Bike series (aggregate nearby bike stations)
        links = self._links[self._links["metro_station_id"] == metro_station_id].copy()
        if links.empty:
            raise KeyError(metro_station_id)
        bike_ids = set(links["bike_station_id"].astype(str).tolist())

        bike_df = self._bike_ts[self._bike_ts["station_id"].astype(str).isin(bike_ids)].copy()
        if not bike_df.empty and ("rent_proxy" not in bike_df.columns or "return_proxy" not in bike_df.columns):
            bike_df = compute_rent_return_proxy(
                bike_df,
                station_id_col="station_id",
                ts_col="ts",
                available_bikes_col="available_bikes",
            )

        bike_available_points: list[dict[str, Any]] = []
        metro_proxy_points: list[dict[str, Any]] = []
        if not bike_df.empty:
            # Availability is a state variable: resample per station (mean), then sum across stations.
            bike_available = align_timeseries(
                bike_df,
                ts_col="ts",
                group_cols=("station_id",),
                value_cols=("available_bikes",),
                granularity=self._config.temporal.granularity,
                timezone=self._config.temporal.timezone,
                agg="mean",
            )
            bike_available = bike_available.groupby("ts", as_index=False)["available_bikes"].sum()
            bike_available = bike_available.sort_values("ts")
            bike_available_points = [
                {"ts": row["ts"], "value": float(row["available_bikes"])}
                for _, row in bike_available.iterrows()
            ]

            # Rent proxy is an event-like signal: sum within buckets per station, then sum across stations.
            if "rent_proxy" in bike_df.columns:
                bike_rent = align_timeseries(
                    bike_df,
                    ts_col="ts",
                    group_cols=("station_id",),
                    value_cols=("rent_proxy",),
                    granularity=self._config.temporal.granularity,
                    timezone=self._config.temporal.timezone,
                    agg="sum",
                )
                bike_rent = bike_rent.groupby("ts", as_index=False)["rent_proxy"].sum()
                bike_rent = bike_rent.sort_values("ts")
                metro_proxy_points = [
                    {"ts": row["ts"], "value": float(row["rent_proxy"])}
                    for _, row in bike_rent.iterrows()
                ]

        # Metro series: prefer real ridership if provided, otherwise fall back to the bike-derived proxy.
        metro_is_proxy = True
        metro_metric = "metro_flow_proxy_from_bike_rent"
        metro_source = "bike_proxy"
        metro_points: list[dict[str, Any]] = metro_proxy_points
        if self._metro_ts is not None and not self._metro_ts.empty:
            metro_df = self._metro_ts[self._metro_ts["station_id"] == metro_station_id].copy()
            if not metro_df.empty:
                metro_is_proxy = False
                metro_metric = "metro_ridership"
                metro_source = "metro_ridership"
                metro_points = [
                    {"ts": row["ts"], "value": float(row["value"])}
                    for _, row in metro_df.sort_values("ts").iterrows()
                ]

        return {
            "station_id": metro_station_id,
            "granularity": self._config.temporal.granularity,
            "timezone": self._config.temporal.timezone,
            "series": [
                {
                    "metric": metro_metric,
                    "points": metro_points,
                    "source": metro_source,
                    "is_proxy": metro_is_proxy,
                },
                {
                    "metric": "bike_available_bikes_total",
                    "points": bike_available_points,
                    "source": "tdx_bike_availability",
                    "is_proxy": False,
                },
            ],
        }

    def station_factors(self, metro_station_id: str) -> dict[str, Any]:
        if self._station_features is None or self._station_features.empty:
            return {"station_id": metro_station_id, "factors": [], "available": False}

        df = self._station_features.copy()
        df["station_id"] = df["station_id"].astype(str)
        if metro_station_id not in set(df["station_id"]):
            raise KeyError(metro_station_id)

        row = df[df["station_id"] == metro_station_id].iloc[0].to_dict()
        numeric_cols = [
            c
            for c in df.columns
            if c != "station_id" and pd.api.types.is_numeric_dtype(df[c])
        ]

        factors = []
        for k, v in row.items():
            if k == "station_id":
                continue
            if pd.isna(v):
                value = None
            else:
                value = v

            percentile = None
            if k in numeric_cols and value is not None:
                pct_series = df[k].rank(pct=True, method="average")
                percentile = float(pct_series[df["station_id"] == metro_station_id].iloc[0])

            factors.append({"name": k, "value": value, "percentile": percentile})

        factors = sorted(factors, key=lambda x: x["name"])
        return {"station_id": metro_station_id, "factors": factors, "available": True}

    def similar_stations(self, metro_station_id: str) -> list[dict[str, Any]]:
        if self._station_features is None or self._station_features.empty:
            return []

        sim = find_similar_stations(
            self._station_features,
            station_id=metro_station_id,
            top_k=self._config.analytics.similarity.top_k,
            metric=self._config.analytics.similarity.metric,
            standardize=self._config.analytics.similarity.standardize,
        )
        merged = sim.merge(
            self._metro_stations[["station_id", "name"]],
            on="station_id",
            how="left",
        )

        cluster_map = None
        if self._station_clusters is not None and not self._station_clusters.empty:
            cluster_df = self._station_clusters.copy()
            if {"station_id", "cluster"} <= set(cluster_df.columns):
                cluster_map = dict(
                    zip(cluster_df["station_id"].astype(str), cluster_df["cluster"].astype(int))
                )

        out = []
        for _, r in merged.iterrows():
            station_id = str(r["station_id"])
            item = {
                "station_id": station_id,
                "name": None if pd.isna(r.get("name")) else str(r.get("name")),
                "distance": float(r["distance"]),
            }
            if cluster_map is not None and station_id in cluster_map:
                item["cluster"] = int(cluster_map[station_id])
            out.append(item)
        return out

    def analytics_overview(self, *, top_n: int = 20) -> dict[str, Any]:
        """
        Return a lightweight overview of precomputed analytics outputs (if present).
        """

        available = self._feature_correlations is not None and self._regression_coefficients is not None
        if not available:
            return {"available": False}

        corr = self._feature_correlations.copy()
        corr = corr.sort_values("correlation", ascending=False).head(max(int(top_n), 0))
        correlations = corr.to_dict(orient="records")

        reg = self._regression_coefficients.copy()
        reg = reg[reg["feature"] != "__intercept__"].copy()
        coefficients = reg[["feature", "coefficient"]].to_dict(orient="records")

        r2 = None
        n = None
        if "r2" in self._regression_coefficients.columns:
            try:
                r2 = float(self._regression_coefficients["r2"].dropna().iloc[0])
            except Exception:
                r2 = None
        if "n" in self._regression_coefficients.columns:
            try:
                n = int(self._regression_coefficients["n"].dropna().iloc[0])
            except Exception:
                n = None

        cluster_counts = None
        if self._station_clusters is not None and {"cluster"} <= set(self._station_clusters.columns):
            counts = self._station_clusters["cluster"].value_counts().sort_index()
            cluster_counts = {int(k): int(v) for k, v in counts.to_dict().items()}

        return {
            "available": True,
            "correlations": correlations,
            "regression": {
                "r2": r2,
                "n": n,
                "coefficients": coefficients,
            },
            "clusters": cluster_counts,
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

    @staticmethod
    def _read_optional_path(path: Path) -> pd.DataFrame | None:
        if not path.exists():
            return None
        return pd.read_csv(path)

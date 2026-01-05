from __future__ import annotations

from pathlib import Path
from typing import Any, Literal, Optional

import pandas as pd

from metrobikeatlas.analytics.similarity import find_similar_stations
from metrobikeatlas.config.models import AppConfig
from metrobikeatlas.preprocessing.temporal_align import align_timeseries, compute_rent_return_proxy
from metrobikeatlas.utils.geo import haversine_m


SpatialJoinMethod = Literal["buffer", "nearest"]
Granularity = Literal["15min", "hour", "day"]
SimilarityMetric = Literal["euclidean", "cosine"]
MetroSeriesMode = Literal["auto", "ridership", "proxy"]


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

    def nearby_bike(
        self,
        metro_station_id: str,
        *,
        join_method: Optional[SpatialJoinMethod] = None,
        radius_m: Optional[float] = None,
        nearest_k: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        bikes = self._nearby_bike_df(
            metro_station_id,
            join_method=join_method,
            radius_m=radius_m,
            nearest_k=nearest_k,
            limit=limit,
        )
        out = []
        for _, row in bikes.iterrows():
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

    def station_timeseries(
        self,
        metro_station_id: str,
        *,
        join_method: Optional[SpatialJoinMethod] = None,
        radius_m: Optional[float] = None,
        nearest_k: Optional[int] = None,
        granularity: Optional[Granularity] = None,
        timezone: Optional[str] = None,
        window_days: Optional[int] = None,
        metro_series: MetroSeriesMode = "auto",
    ) -> dict[str, Any]:
        """
        Return aligned time series for a metro station.

        - Spatial parameters choose which bike stations to aggregate.
        - Temporal parameters define resampling granularity/timezone.
        """

        nearby = self._nearby_bike_df(
            metro_station_id,
            join_method=join_method,
            radius_m=radius_m,
            nearest_k=nearest_k,
            limit=None,
        )
        bike_ids = set(nearby["station_id"].astype(str).tolist())

        gran, tz = self._resolve_temporal(granularity=granularity, timezone=timezone)
        window_days_value = None if window_days is None else max(int(window_days), 1)

        bike_df = self._bike_ts[self._bike_ts["station_id"].astype(str).isin(bike_ids)].copy()
        if window_days_value is not None and not bike_df.empty:
            bike_df["ts"] = pd.to_datetime(bike_df["ts"], utc=True, errors="coerce")
            end_ts = bike_df["ts"].max()
            if pd.notna(end_ts):
                bike_df = bike_df[bike_df["ts"] >= (end_ts - pd.Timedelta(days=window_days_value))].copy()

        if not bike_df.empty and ("rent_proxy" not in bike_df.columns or "return_proxy" not in bike_df.columns):
            bike_df = compute_rent_return_proxy(
                bike_df,
                station_id_col="station_id",
                ts_col="ts",
                available_bikes_col="available_bikes",
            )

        bike_available_points: list[dict[str, Any]] = []
        bike_rent_points: list[dict[str, Any]] = []
        bike_return_points: list[dict[str, Any]] = []
        metro_proxy_points: list[dict[str, Any]] = []

        if not bike_df.empty:
            bike_available = align_timeseries(
                bike_df,
                ts_col="ts",
                group_cols=("station_id",),
                value_cols=("available_bikes",),
                granularity=gran,
                timezone=tz,
                agg="mean",
            )
            bike_available = bike_available.groupby("ts", as_index=False)["available_bikes"].sum().sort_values("ts")
            bike_available_points = [
                {"ts": row["ts"], "value": float(row["available_bikes"])}
                for _, row in bike_available.iterrows()
            ]

            bike_rent = align_timeseries(
                bike_df,
                ts_col="ts",
                group_cols=("station_id",),
                value_cols=("rent_proxy",),
                granularity=gran,
                timezone=tz,
                agg="sum",
            )
            bike_rent = bike_rent.groupby("ts", as_index=False)["rent_proxy"].sum().sort_values("ts")
            bike_rent_points = [
                {"ts": row["ts"], "value": float(row["rent_proxy"])}
                for _, row in bike_rent.iterrows()
            ]

            bike_return = align_timeseries(
                bike_df,
                ts_col="ts",
                group_cols=("station_id",),
                value_cols=("return_proxy",),
                granularity=gran,
                timezone=tz,
                agg="sum",
            )
            bike_return = (
                bike_return.groupby("ts", as_index=False)["return_proxy"].sum().sort_values("ts")
            )
            bike_return_points = [
                {"ts": row["ts"], "value": float(row["return_proxy"])}
                for _, row in bike_return.iterrows()
            ]

            metro_proxy_points = bike_rent_points

        metro_ridership_points: list[dict[str, Any]] = []
        if self._metro_ts is not None and not self._metro_ts.empty:
            metro_df = self._metro_ts[self._metro_ts["station_id"] == metro_station_id].copy()
            if not metro_df.empty:
                if window_days_value is not None:
                    metro_df["ts"] = pd.to_datetime(metro_df["ts"], utc=True, errors="coerce")
                    end_ts = metro_df["ts"].max()
                    if pd.notna(end_ts):
                        metro_df = metro_df[metro_df["ts"] >= (end_ts - pd.Timedelta(days=window_days_value))].copy()

                metro_df = align_timeseries(
                    metro_df,
                    ts_col="ts",
                    group_cols=("station_id",),
                    value_cols=("value",),
                    granularity=gran,
                    timezone=tz,
                    agg="sum",
                )
                metro_df = metro_df.sort_values("ts")
                metro_ridership_points = [
                    {"ts": row["ts"], "value": float(row["value"])}
                    for _, row in metro_df.iterrows()
                ]

        series: list[dict[str, Any]] = []
        if metro_series in ("auto", "ridership") and metro_ridership_points:
            series.append(
                {
                    "metric": "metro_ridership",
                    "points": metro_ridership_points,
                    "source": "metro_ridership",
                    "is_proxy": False,
                }
            )
        series.append(
            {
                "metric": "metro_flow_proxy_from_bike_rent",
                "points": metro_proxy_points,
                "source": "bike_proxy",
                "is_proxy": True,
            }
        )
        if metro_series == "proxy" and metro_ridership_points:
            series.insert(
                0,
                series.pop(),  # move proxy first
            )
            series.insert(
                1,
                {
                    "metric": "metro_ridership",
                    "points": metro_ridership_points,
                    "source": "metro_ridership",
                    "is_proxy": False,
                },
            )

        series.extend(
            [
                {
                    "metric": "bike_available_bikes_total",
                    "points": bike_available_points,
                    "source": "tdx_bike_availability",
                    "is_proxy": False,
                },
                {
                    "metric": "bike_rent_proxy_total",
                    "points": bike_rent_points,
                    "source": "tdx_bike_availability",
                    "is_proxy": True,
                },
                {
                    "metric": "bike_return_proxy_total",
                    "points": bike_return_points,
                    "source": "tdx_bike_availability",
                    "is_proxy": True,
                },
            ]
        )

        return {
            "station_id": metro_station_id,
            "granularity": gran,
            "timezone": tz,
            "series": series,
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

    def similar_stations(
        self,
        metro_station_id: str,
        *,
        top_k: Optional[int] = None,
        metric: Optional[SimilarityMetric] = None,
        standardize: Optional[bool] = None,
    ) -> list[dict[str, Any]]:
        if self._station_features is None or self._station_features.empty:
            return []

        top_k_value = self._config.analytics.similarity.top_k if top_k is None else int(top_k)
        metric_value = self._config.analytics.similarity.metric if metric is None else metric
        standardize_value = (
            self._config.analytics.similarity.standardize if standardize is None else bool(standardize)
        )

        sim = find_similar_stations(
            self._station_features,
            station_id=metro_station_id,
            top_k=top_k_value,
            metric=metric_value,
            standardize=standardize_value,
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

    def _resolve_spatial(
        self,
        *,
        join_method: Optional[SpatialJoinMethod] = None,
        radius_m: Optional[float] = None,
        nearest_k: Optional[int] = None,
    ) -> tuple[SpatialJoinMethod, float, int]:
        method: SpatialJoinMethod = join_method or self._config.spatial.join_method
        if method not in ("buffer", "nearest"):
            raise ValueError(f"Unsupported join_method: {method}")

        r = self._config.spatial.radius_m if radius_m is None else float(radius_m)
        k = self._config.spatial.nearest_k if nearest_k is None else int(nearest_k)
        return method, float(r), int(k)

    def _resolve_temporal(
        self,
        *,
        granularity: Optional[Granularity] = None,
        timezone: Optional[str] = None,
    ) -> tuple[Granularity, str]:
        gran: Granularity = granularity or self._config.temporal.granularity
        if gran not in ("15min", "hour", "day"):
            raise ValueError(f"Unsupported granularity: {gran}")
        tz = self._config.temporal.timezone if timezone is None else str(timezone)
        return gran, tz

    def _nearby_bike_df(
        self,
        metro_station_id: str,
        *,
        join_method: Optional[SpatialJoinMethod] = None,
        radius_m: Optional[float] = None,
        nearest_k: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        metro = self._metro_stations.copy()
        metro["station_id"] = metro["station_id"].astype(str)
        hit = metro[metro["station_id"] == str(metro_station_id)]
        if hit.empty:
            raise KeyError(metro_station_id)

        row = hit.iloc[0]
        lat = float(row["lat"])
        lon = float(row["lon"])

        method, radius, k = self._resolve_spatial(
            join_method=join_method,
            radius_m=radius_m,
            nearest_k=nearest_k,
        )

        bikes = self._bike_stations.copy()
        bikes["station_id"] = bikes["station_id"].astype(str)
        bikes["distance_m"] = bikes.apply(
            lambda r: haversine_m(lat, lon, float(r["lat"]), float(r["lon"])), axis=1
        )

        if method == "buffer":
            bikes = bikes[bikes["distance_m"] <= float(radius)].copy()
        else:
            bikes = bikes.nsmallest(max(int(k), 1), "distance_m").copy()

        bikes = bikes.sort_values("distance_m")
        if limit is not None:
            bikes = bikes.head(max(int(limit), 0))
        return bikes

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

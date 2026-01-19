from __future__ import annotations

from pathlib import Path
from typing import Any, Literal, Optional

import os
import pandas as pd
import sqlite3

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
        self._bike_ts_parts_dir = self._silver_dir / "bike_timeseries_parts"
        self._lazy_bike_ts = str(os.getenv("METROBIKEATLAS_LAZY_BIKE_TS", "")).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self._sqlite_path = self._silver_dir / "metrobikeatlas.db"
        self._use_sqlite = str(os.getenv("METROBIKEATLAS_STORAGE", "")).strip().lower() == "sqlite" and self._sqlite_path.exists()
        if not self._lazy_bike_ts:
            self._bike_ts = self._read_required_csv("bike_timeseries.csv", parse_dates=["ts"])
        else:
            # In lazy mode, allow either partitioned files or the monolithic CSV.
            bike_ts_csv = self._silver_dir / "bike_timeseries.csv"
            if not self._bike_ts_parts_dir.exists() and not bike_ts_csv.exists():
                raise FileNotFoundError(f"Missing required file: {bike_ts_csv}")
            self._bike_ts = None
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

    def _read_bike_ts_lazy(self, *, bike_ids: set[str], window_days: int | None) -> pd.DataFrame:
        """
        Read bike time series in a scalable way for long-running deployments.

        If `data/silver/bike_timeseries_parts/YYYY-MM-DD.csv` exists, we read only the most recent days.
        Otherwise we fall back to reading the full `bike_timeseries.csv`.
        """

        if self._use_sqlite:
            return self._read_bike_ts_sqlite(bike_ids=bike_ids, window_days=window_days)

        if self._bike_ts_parts_dir.exists():
            files = sorted(self._bike_ts_parts_dir.glob("*.csv"))
            if not files:
                return pd.DataFrame(columns=["station_id", "ts"])
            if window_days is not None:
                use = files[-max(int(window_days) + 1, 1) :]
            else:
                use = files
            frames = []
            for f in use:
                try:
                    df = pd.read_csv(f, parse_dates=["ts"])
                except Exception:
                    continue
                if "station_id" in df.columns:
                    df["station_id"] = df["station_id"].astype(str)
                    df = df[df["station_id"].astype(str).isin(bike_ids)].copy()
                frames.append(df)
            if not frames:
                return pd.DataFrame(columns=["station_id", "ts"])
            out = pd.concat(frames, ignore_index=True)
            out["ts"] = pd.to_datetime(out["ts"], utc=True, errors="coerce")
            return out

        # Fallback: monolithic CSV
        df = self._read_required_csv("bike_timeseries.csv", parse_dates=["ts"])
        df["station_id"] = df["station_id"].astype(str)
        return df[df["station_id"].astype(str).isin(bike_ids)].copy()

    def _read_bike_ts_sqlite(self, *, bike_ids: set[str], window_days: int | None) -> pd.DataFrame:
        if not bike_ids:
            return pd.DataFrame(columns=["station_id", "ts"])
        conn = sqlite3.connect(str(self._sqlite_path))
        try:
            # Find end ts for this selection.
            q_marks = ",".join(["?"] * len(bike_ids))
            cur = conn.cursor()
            cur.execute(f"SELECT MAX(ts) FROM bike_timeseries WHERE station_id IN ({q_marks})", tuple(bike_ids))
            row = cur.fetchone()
            end_ts = row[0] if row else None
            where_time = ""
            params = list(bike_ids)
            if end_ts and window_days is not None:
                # Use lexical comparison on ISO strings (works for fixed format).
                cur.execute("SELECT datetime(?, ?)", (end_ts, f"-{int(window_days)} days"))
                start_row = cur.fetchone()
                start_ts = start_row[0] if start_row else None
                if start_ts:
                    where_time = " AND ts >= ?"
                    params.append(start_ts)
            sql = f"SELECT station_id, ts, city, available_bikes, available_docks, rent_proxy, return_proxy FROM bike_timeseries WHERE station_id IN ({q_marks}){where_time}"
            df = pd.read_sql_query(sql, conn, params=params)
        finally:
            conn.close()
        if df.empty:
            return df
        df["station_id"] = df["station_id"].astype(str)
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
        return df.dropna(subset=["ts"]).copy()

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

        out = df.to_dict(orient="records")
        for row in out:
            row["source"] = "silver"
        return out

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

        if self._lazy_bike_ts:
            bike_df = self._read_bike_ts_lazy(bike_ids=bike_ids, window_days=window_days_value)
        else:
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

    def metro_bike_availability_index(self, *, limit: int = 200) -> list[dict[str, Any]]:
        if self._lazy_bike_ts:
            # Prefer partitioned files: read a small suffix of files to get a recent index.
            if self._bike_ts_parts_dir.exists():
                files = sorted(self._bike_ts_parts_dir.glob("*.csv"))[-7:]  # last week
                stamps = []
                for f in files:
                    try:
                        df = pd.read_csv(f, usecols=["ts"])
                    except Exception:
                        continue
                    t = pd.to_datetime(df["ts"], utc=True, errors="coerce")
                    stamps.extend([x for x in t.dropna().unique()])
                stamps = sorted(set(stamps))[-max(int(limit), 1) :]
                return [{"ts": pd.to_datetime(t, utc=True).to_pydatetime()} for t in stamps]
            # SQLite fallback: ask for global distinct ts (bounded).
            if self._use_sqlite:
                conn = sqlite3.connect(str(self._sqlite_path))
                try:
                    rows = conn.execute(
                        "SELECT DISTINCT ts FROM bike_timeseries ORDER BY ts DESC LIMIT ?",
                        (max(int(limit), 1),),
                    ).fetchall()
                finally:
                    conn.close()
                stamps = [pd.to_datetime(r[0], utc=True, errors="coerce") for r in rows if r and r[0]]
                stamps = [t for t in stamps if pd.notna(t)]
                stamps = list(reversed(stamps))
                return [{"ts": t.to_pydatetime()} for t in stamps]
            return []

        df = self._bike_ts.copy()
        if df.empty or "ts" not in df.columns:
            return []
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
        df = df.dropna(subset=["ts"])
        if df.empty:
            return []
        timestamps = sorted(df["ts"].unique())
        timestamps = timestamps[-max(int(limit), 1) :]
        return [{"ts": t.to_pydatetime()} for t in timestamps]

    def metro_bike_availability_at(self, ts: str) -> list[dict[str, Any]]:
        if self._lazy_bike_ts:
            # Read only the relevant day partition when available.
            target = pd.to_datetime(ts, utc=True, errors="coerce")
            if pd.isna(target):
                raise ValueError(f"Invalid ts: {ts}")
            if self._bike_ts_parts_dir.exists():
                day = target.strftime("%Y-%m-%d")
                f = self._bike_ts_parts_dir / f"{day}.csv"
                if f.exists():
                    df = pd.read_csv(f, parse_dates=["ts"])
                else:
                    return []
            elif self._use_sqlite:
                conn = sqlite3.connect(str(self._sqlite_path))
                try:
                    rows = conn.execute(
                        "SELECT station_id, ts, available_bikes FROM bike_timeseries WHERE ts = ?",
                        (target.strftime("%Y-%m-%dT%H:%M:%S%z"),),
                    ).fetchall()
                finally:
                    conn.close()
                df = pd.DataFrame(rows, columns=["station_id", "ts", "available_bikes"])
            else:
                return []
        else:
            df = self._bike_ts.copy()
        if df.empty:
            return []
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
        target = pd.to_datetime(ts, utc=True, errors="coerce")
        if pd.isna(target):
            raise ValueError(f"Invalid ts: {ts}")

        at = df[df["ts"] == target].copy()
        if at.empty:
            return []
        if "station_id" not in at.columns or "available_bikes" not in at.columns:
            return []

        at["station_id"] = at["station_id"].astype(str)
        links = self._links.copy()
        links["metro_station_id"] = links["metro_station_id"].astype(str)
        links["bike_station_id"] = links["bike_station_id"].astype(str)

        merged = links.merge(at, left_on="bike_station_id", right_on="station_id", how="left")
        merged["available_bikes"] = pd.to_numeric(merged["available_bikes"], errors="coerce").fillna(0.0)
        agg = merged.groupby("metro_station_id", as_index=False)["available_bikes"].sum()

        return [
            {
                "station_id": str(row["metro_station_id"]),
                "ts": target.to_pydatetime(),
                "available_bikes_total": float(row["available_bikes"]),
            }
            for _, row in agg.iterrows()
        ]

    def metro_heat_index(self, *, limit: int = 200) -> list[dict[str, Any]]:
        # Heat timestamp index is shared across metrics because all metrics come from `bike_timeseries.csv`.
        return self.metro_bike_availability_index(limit=limit)

    def metro_heat_at(self, ts: str, *, metric: str = "available", agg: str = "sum") -> list[dict[str, Any]]:
        if self._lazy_bike_ts:
            target = pd.to_datetime(ts, utc=True, errors="coerce")
            if pd.isna(target):
                raise ValueError(f"Invalid ts: {ts}")
            if self._bike_ts_parts_dir.exists():
                day = target.strftime("%Y-%m-%d")
                f = self._bike_ts_parts_dir / f"{day}.csv"
                if not f.exists():
                    return []
                df = pd.read_csv(f, parse_dates=["ts"])
            elif self._use_sqlite:
                metric_key = str(metric).strip().lower()
                metric_col = {
                    "available": "available_bikes",
                    "rent_proxy": "rent_proxy",
                    "return_proxy": "return_proxy",
                }.get(metric_key)
                if metric_col is None:
                    raise ValueError(f"Unsupported metric: {metric}")
                conn = sqlite3.connect(str(self._sqlite_path))
                try:
                    rows = conn.execute(
                        f"SELECT station_id, ts, {metric_col} FROM bike_timeseries WHERE ts = ?",
                        (target.strftime("%Y-%m-%dT%H:%M:%S%z"),),
                    ).fetchall()
                finally:
                    conn.close()
                df = pd.DataFrame(rows, columns=["station_id", "ts", metric_col])
            else:
                return []
        else:
            df = self._bike_ts.copy()
        if df.empty:
            return []

        metric_key = str(metric).strip().lower()
        metric_col = {
            "available": "available_bikes",
            "rent_proxy": "rent_proxy",
            "return_proxy": "return_proxy",
        }.get(metric_key)
        if metric_col is None:
            raise ValueError(f"Unsupported metric: {metric}")

        # Ensure proxy columns exist when requested.
        if metric_col in {"rent_proxy", "return_proxy"} and (
            metric_col not in df.columns or "rent_proxy" not in df.columns or "return_proxy" not in df.columns
        ):
            df = compute_rent_return_proxy(
                df,
                station_id_col="station_id",
                ts_col="ts",
                available_bikes_col="available_bikes",
            )
            self._bike_ts = df

        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
        target = pd.to_datetime(ts, utc=True, errors="coerce")
        if pd.isna(target):
            raise ValueError(f"Invalid ts: {ts}")

        at = df[df["ts"] == target].copy()
        if at.empty:
            return []
        if "station_id" not in at.columns or metric_col not in at.columns:
            return []

        at["station_id"] = at["station_id"].astype(str)
        links = self._links.copy()
        links["metro_station_id"] = links["metro_station_id"].astype(str)
        links["bike_station_id"] = links["bike_station_id"].astype(str)

        merged = links.merge(at, left_on="bike_station_id", right_on="station_id", how="left")
        merged[metric_col] = pd.to_numeric(merged[metric_col], errors="coerce").fillna(0.0)

        agg_key = str(agg).strip().lower()
        if agg_key == "sum":
            grouped = merged.groupby("metro_station_id", as_index=False)[metric_col].sum()
        elif agg_key == "mean":
            grouped = merged.groupby("metro_station_id", as_index=False)[metric_col].mean()
        else:
            raise ValueError(f"Unsupported agg: {agg}")

        return [
            {
                "station_id": str(row["metro_station_id"]),
                "ts": target.to_pydatetime(),
                "metric": metric_key,
                "agg": agg_key,
                "value": float(row[metric_col]),
            }
            for _, row in grouped.iterrows()
        ]

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

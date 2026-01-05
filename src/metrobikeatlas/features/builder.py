from __future__ import annotations

from dataclasses import dataclass
import logging
import math
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

from metrobikeatlas.config.models import AppConfig
from metrobikeatlas.preprocessing.temporal_align import compute_rent_return_proxy
from metrobikeatlas.utils.geo import haversine_m


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FeatureArtifacts:
    station_features: pd.DataFrame
    station_targets: pd.DataFrame


def load_poi_csv(path: Path) -> pd.DataFrame:
    """
    Expected columns (CSV):
    - `poi_id` (optional), `name` (optional), `category`, `lat`, `lon`
    """

    df = pd.read_csv(path)
    required = {"category", "lat", "lon"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"POI CSV missing columns: {sorted(missing)}")
    df["category"] = df["category"].astype(str)
    df["lat"] = df["lat"].astype(float)
    df["lon"] = df["lon"].astype(float)
    return df


def load_station_district_map_csv(path: Path) -> pd.DataFrame:
    """
    Expected columns (CSV):
    - `station_id`, `district`
    """

    df = pd.read_csv(path)
    required = {"station_id", "district"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"District map CSV missing columns: {sorted(missing)}")
    df["station_id"] = df["station_id"].astype(str)
    df["district"] = df["district"].astype(str)
    return df


def _bike_feature_suffix(config: AppConfig) -> str:
    if config.spatial.join_method == "buffer":
        return f"r{int(config.spatial.radius_m)}m"
    return f"k{int(config.spatial.nearest_k)}"


def build_station_features(
    *,
    config: AppConfig,
    metro_stations: pd.DataFrame,
    bike_stations: pd.DataFrame,
    links: pd.DataFrame,
    bike_timeseries: Optional[pd.DataFrame] = None,
    poi: Optional[pd.DataFrame] = None,
    district_map: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Build station-level (metro) feature table.

    This is designed for simple EDA and similarity search, not for final ML.
    """

    if metro_stations.empty:
        raise ValueError("metro_stations is empty")

    suffix = _bike_feature_suffix(config)
    features = pd.DataFrame({"station_id": metro_stations["station_id"].astype(str)})

    # District (optional)
    if district_map is not None and not district_map.empty:
        features = features.merge(district_map[["station_id", "district"]], on="station_id", how="left")

    # Bike access features from the metro↔bike mapping
    if not links.empty:
        bikes = bike_stations.copy()
        bikes["station_id"] = bikes["station_id"].astype(str)
        if "capacity" in bikes.columns:
            bikes["capacity"] = pd.to_numeric(bikes["capacity"], errors="coerce")
        else:
            bikes["capacity"] = math.nan

        joined = links.merge(
            bikes[["station_id", "capacity"]],
            left_on="bike_station_id",
            right_on="station_id",
            how="left",
            suffixes=("", "_bike"),
        )

        grouped = joined.groupby("metro_station_id", as_index=False).agg(
            bike_station_count=("bike_station_id", "nunique"),
            bike_capacity_sum=("capacity", "sum"),
            bike_distance_mean_m=("distance_m", "mean"),
        )
        grouped = grouped.rename(columns={"metro_station_id": "station_id"})

        features = features.merge(grouped, on="station_id", how="left")
    else:
        features["bike_station_count"] = 0
        features["bike_capacity_sum"] = math.nan
        features["bike_distance_mean_m"] = math.nan

    # Rename with suffix to make parameterization explicit
    rename_map = {
        "bike_station_count": f"bike_station_count_{suffix}",
        "bike_capacity_sum": f"bike_capacity_sum_{suffix}",
        "bike_distance_mean_m": f"bike_distance_mean_m_{suffix}",
    }
    features = features.rename(columns=rename_map)

    # Optional composite score (weights are config-driven)
    count_col = rename_map["bike_station_count"]
    capacity_col = rename_map["bike_capacity_sum"]
    distance_col = rename_map["bike_distance_mean_m"]
    weights = config.features.accessibility.bike
    score_col = f"bike_accessibility_score_{suffix}"
    features[score_col] = (
        float(weights.bias)
        + float(weights.w_station_count) * pd.to_numeric(features[count_col], errors="coerce").fillna(0.0)
        + float(weights.w_capacity_sum) * pd.to_numeric(features[capacity_col], errors="coerce").fillna(0.0)
        + float(weights.w_distance_mean_m) * pd.to_numeric(features[distance_col], errors="coerce").fillna(0.0)
    )

    # POI features (optional)
    if poi is not None and not poi.empty and config.features.poi is not None:
        poi_cfg = config.features.poi
        categories = set(poi_cfg.categories)

        for radius_m in poi_cfg.radii_m:
            total_col = f"poi_count_{int(radius_m)}m"
            density_col = f"poi_density_{int(radius_m)}m_per_km2"
            features[total_col] = 0
            features[density_col] = 0.0
            for cat in categories:
                features[f"poi_count_{cat}_{int(radius_m)}m"] = 0

        poi_lat = poi["lat"].astype(float)
        poi_lon = poi["lon"].astype(float)

        for _, station in metro_stations.iterrows():
            station_id = str(station["station_id"])
            lat0 = float(station["lat"])
            lon0 = float(station["lon"])

            # Pre-filter using an approximate bounding box to reduce computations.
            for radius_m in config.features.poi.radii_m:
                lat_deg = radius_m / 111_000.0
                lon_deg = radius_m / (111_000.0 * max(math.cos(math.radians(lat0)), 1e-6))

                mask = (
                    (poi_lat >= lat0 - lat_deg)
                    & (poi_lat <= lat0 + lat_deg)
                    & (poi_lon >= lon0 - lon_deg)
                    & (poi_lon <= lon0 + lon_deg)
                )
                candidates = poi[mask].copy()
                if candidates.empty:
                    continue

                distances = candidates.apply(
                    lambda r: haversine_m(lat0, lon0, float(r["lat"]), float(r["lon"])), axis=1
                )
                within = candidates[distances <= radius_m]
                if within.empty:
                    continue

                total_col = f"poi_count_{int(radius_m)}m"
                features.loc[features["station_id"] == station_id, total_col] = int(len(within))

                area_km2 = math.pi * (radius_m / 1000.0) ** 2
                density_col = f"poi_density_{int(radius_m)}m_per_km2"
                features.loc[features["station_id"] == station_id, density_col] = float(
                    len(within) / max(area_km2, 1e-9)
                )

                within_counts = within["category"].astype(str).value_counts()
                for cat in categories:
                    col = f"poi_count_{cat}_{int(radius_m)}m"
                    features.loc[features["station_id"] == station_id, col] = int(within_counts.get(cat, 0))

    # Time-pattern features from bike rent proxy (optional)
    if bike_timeseries is not None and not bike_timeseries.empty and not links.empty:
        pattern = _build_time_pattern_features(
            config=config,
            bike_timeseries=bike_timeseries,
            links=links,
            suffix=suffix,
        )
        features = features.merge(pattern, on="station_id", how="left")

    return features


def _build_time_pattern_features(
    *,
    config: AppConfig,
    bike_timeseries: pd.DataFrame,
    links: pd.DataFrame,
    suffix: str,
) -> pd.DataFrame:
    ts = bike_timeseries.copy()
    ts["station_id"] = ts["station_id"].astype(str)
    ts["ts"] = pd.to_datetime(ts["ts"], utc=True, errors="coerce")
    ts = ts.dropna(subset=["ts"])

    if ts.empty:
        return pd.DataFrame(columns=["station_id"])

    if "rent_proxy" not in ts.columns or "return_proxy" not in ts.columns:
        ts = compute_rent_return_proxy(
            ts,
            station_id_col="station_id",
            ts_col="ts",
            available_bikes_col="available_bikes",
        )

    end_ts = ts["ts"].max()
    window_days = int(config.features.timeseries_window_days)
    window_start = end_ts - pd.Timedelta(days=window_days)
    ts = ts[ts["ts"] >= window_start].copy()

    links_sorted = links.sort_values("distance_m").copy()
    links_sorted["bike_station_id"] = links_sorted["bike_station_id"].astype(str)
    primary = links_sorted.drop_duplicates(subset=["bike_station_id"], keep="first")[
        ["bike_station_id", "metro_station_id"]
    ].copy()
    primary = primary.rename(columns={"bike_station_id": "station_id", "metro_station_id": "station_id_metro"})

    joined = ts.merge(primary, on="station_id", how="inner")
    if joined.empty:
        return pd.DataFrame(columns=["station_id"])

    local_ts = joined["ts"].dt.tz_convert(config.temporal.timezone)
    joined["hour"] = local_ts.dt.hour.astype(int)
    joined["weekday"] = local_ts.dt.weekday.astype(int)
    joined["is_weekend"] = (joined["weekday"] >= 5).astype(int)

    tp = config.features.time_patterns
    joined["is_peak_am"] = (
        (joined["hour"] >= int(tp.peak_am_start_hour)) & (joined["hour"] < int(tp.peak_am_end_hour))
    ).astype(int)
    joined["is_peak_pm"] = (
        (joined["hour"] >= int(tp.peak_pm_start_hour)) & (joined["hour"] < int(tp.peak_pm_end_hour))
    ).astype(int)

    joined["rent_total"] = joined["rent_proxy"].astype(float)
    joined["rent_weekend"] = joined["rent_proxy"].astype(float) * joined["is_weekend"].astype(float)
    joined["rent_peak_am"] = joined["rent_proxy"].astype(float) * joined["is_peak_am"].astype(float)
    joined["rent_peak_pm"] = joined["rent_proxy"].astype(float) * joined["is_peak_pm"].astype(float)

    grouped = joined.groupby("station_id_metro", as_index=False).agg(
        rent_total=("rent_total", "sum"),
        rent_weekend=("rent_weekend", "sum"),
        rent_peak_am=("rent_peak_am", "sum"),
        rent_peak_pm=("rent_peak_pm", "sum"),
    )
    grouped = grouped.rename(columns={"station_id_metro": "station_id"})

    total = grouped["rent_total"].replace({0.0: pd.NA})
    grouped[f"bike_rent_proxy_total_{suffix}_{window_days}d"] = grouped["rent_total"]
    grouped[f"bike_rent_proxy_weekend_share_{suffix}_{window_days}d"] = (
        grouped["rent_weekend"] / total
    )
    grouped[f"bike_rent_proxy_peak_am_share_{suffix}_{window_days}d"] = (
        grouped["rent_peak_am"] / total
    )
    grouped[f"bike_rent_proxy_peak_pm_share_{suffix}_{window_days}d"] = (
        grouped["rent_peak_pm"] / total
    )

    return grouped[
        [
            "station_id",
            f"bike_rent_proxy_total_{suffix}_{window_days}d",
            f"bike_rent_proxy_weekend_share_{suffix}_{window_days}d",
            f"bike_rent_proxy_peak_am_share_{suffix}_{window_days}d",
            f"bike_rent_proxy_peak_pm_share_{suffix}_{window_days}d",
        ]
    ]


def build_station_targets(
    *,
    config: AppConfig,
    bike_timeseries: pd.DataFrame,
    links: pd.DataFrame,
    metro_timeseries: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Build a station-level target metric used for EDA (not a ground-truth ridership).

    Target definition (MVP):
    - Sum of `rent_proxy` over the last N days for bike stations assigned to the nearest metro station.
    """

    targets: list[pd.DataFrame] = []

    if not bike_timeseries.empty and not links.empty:
        ts = bike_timeseries.copy()
        ts["station_id"] = ts["station_id"].astype(str)
        ts["ts"] = pd.to_datetime(ts["ts"], utc=True, errors="coerce")
        ts = ts.dropna(subset=["ts"])

        if "rent_proxy" not in ts.columns or "return_proxy" not in ts.columns:
            ts = compute_rent_return_proxy(
                ts,
                station_id_col="station_id",
                ts_col="ts",
                available_bikes_col="available_bikes",
            )

        end_ts = ts["ts"].max()
        window_start = end_ts - pd.Timedelta(days=int(config.features.timeseries_window_days))
        ts = ts[ts["ts"] >= window_start].copy()

        # Assign each bike station to the nearest metro station to avoid double counting.
        links_sorted = links.sort_values("distance_m").copy()
        links_sorted["bike_station_id"] = links_sorted["bike_station_id"].astype(str)
        primary = links_sorted.drop_duplicates(subset=["bike_station_id"], keep="first")[
            ["bike_station_id", "metro_station_id"]
        ].copy()
        primary = primary.rename(columns={"bike_station_id": "station_id", "metro_station_id": "station_id_metro"})

        joined = ts.merge(primary, on="station_id", how="inner")
        if not joined.empty:
            grouped = joined.groupby("station_id_metro", as_index=False)["rent_proxy"].sum()
            grouped = grouped.rename(columns={"station_id_metro": "station_id", "rent_proxy": "value"})
            grouped["metric"] = "metro_flow_proxy_from_bike_rent"
            grouped["window_days"] = int(config.features.timeseries_window_days)
            targets.append(grouped[["station_id", "metric", "value", "window_days"]])

    if metro_timeseries is not None and not metro_timeseries.empty:
        mt = metro_timeseries.copy()
        if {"station_id", "ts", "value"} - set(mt.columns):
            logger.warning("metro_timeseries missing required columns; expected station_id, ts, value.")
        else:
            mt["station_id"] = mt["station_id"].astype(str)
            mt["ts"] = pd.to_datetime(mt["ts"], utc=True, errors="coerce")
            mt["value"] = pd.to_numeric(mt["value"], errors="coerce")
            mt = mt.dropna(subset=["ts", "value"])
            if not mt.empty:
                end_ts = mt["ts"].max()
                window_start = end_ts - pd.Timedelta(days=int(config.features.timeseries_window_days))
                mt = mt[mt["ts"] >= window_start].copy()
                grouped = mt.groupby("station_id", as_index=False)["value"].sum()
                grouped["metric"] = "metro_ridership"
                grouped["window_days"] = int(config.features.timeseries_window_days)
                targets.append(grouped[["station_id", "metric", "value", "window_days"]])

    if not targets:
        return pd.DataFrame(columns=["station_id", "metric", "value", "window_days"])

    return pd.concat(targets, ignore_index=True)


def build_feature_artifacts(
    *,
    config: AppConfig,
    metro_stations: pd.DataFrame,
    bike_stations: pd.DataFrame,
    links: pd.DataFrame,
    bike_timeseries: pd.DataFrame,
    metro_timeseries: Optional[pd.DataFrame] = None,
    poi: Optional[pd.DataFrame] = None,
    district_map: Optional[pd.DataFrame] = None,
) -> FeatureArtifacts:
    station_features = build_station_features(
        config=config,
        metro_stations=metro_stations,
        bike_stations=bike_stations,
        links=links,
        bike_timeseries=bike_timeseries,
        poi=poi,
        district_map=district_map,
    )
    station_targets = build_station_targets(
        config=config,
        bike_timeseries=bike_timeseries,
        links=links,
        metro_timeseries=metro_timeseries,
    )
    return FeatureArtifacts(station_features=station_features, station_targets=station_targets)

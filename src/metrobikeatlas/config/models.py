from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional


Granularity = Literal["15min", "hour", "day"]
SpatialJoinMethod = Literal["buffer", "nearest"]
SimilarityMetric = Literal["euclidean", "cosine"]


@dataclass(frozen=True)
class AppSettings:
    name: str = "MetroBikeAtlas"
    demo_mode: bool = True


@dataclass(frozen=True)
class TDXMetroSettings:
    cities: list[str]
    stations_path_template: str
    ridership_path_template: Optional[str]


@dataclass(frozen=True)
class TDXBikeSettings:
    cities: list[str]
    stations_path_template: str
    availability_path_template: str


@dataclass(frozen=True)
class TDXSettings:
    base_url: str
    token_url: str
    metro: TDXMetroSettings
    bike: TDXBikeSettings


@dataclass(frozen=True)
class TemporalSettings:
    timezone: str
    granularity: Granularity


@dataclass(frozen=True)
class SpatialSettings:
    join_method: SpatialJoinMethod
    radius_m: float
    nearest_k: int


@dataclass(frozen=True)
class CacheSettings:
    dir: Path
    ttl_seconds: int


@dataclass(frozen=True)
class POISettings:
    path: Path
    radii_m: list[int]
    categories: list[str]


@dataclass(frozen=True)
class FeatureTimePatternSettings:
    peak_am_start_hour: int
    peak_am_end_hour: int
    peak_pm_start_hour: int
    peak_pm_end_hour: int


@dataclass(frozen=True)
class BikeAccessibilityWeights:
    w_station_count: float
    w_capacity_sum: float
    w_distance_mean_m: float
    bias: float = 0.0


@dataclass(frozen=True)
class AccessibilitySettings:
    bike: BikeAccessibilityWeights


@dataclass(frozen=True)
class FeatureSettings:
    station_features_path: Path
    station_targets_path: Path
    timeseries_window_days: int
    admin_boundaries_geojson_path: Optional[Path]
    time_patterns: FeatureTimePatternSettings
    accessibility: AccessibilitySettings
    poi: Optional[POISettings]
    station_district_map_path: Optional[Path]


@dataclass(frozen=True)
class SimilaritySettings:
    top_k: int
    metric: SimilarityMetric
    standardize: bool


@dataclass(frozen=True)
class ClusteringSettings:
    k: int
    standardize: bool


@dataclass(frozen=True)
class AnalyticsSettings:
    similarity: SimilaritySettings
    clustering: ClusteringSettings


@dataclass(frozen=True)
class LoggingSettings:
    level: str
    format: str
    file: Optional[Path] = None


@dataclass(frozen=True)
class WebMapSettings:
    center_lat: float
    center_lon: float
    zoom: int


@dataclass(frozen=True)
class WebSettings:
    static_dir: Path
    map: WebMapSettings


@dataclass(frozen=True)
class AppConfig:
    app: AppSettings
    tdx: TDXSettings
    temporal: TemporalSettings
    spatial: SpatialSettings
    cache: CacheSettings
    features: FeatureSettings
    analytics: AnalyticsSettings
    logging: LoggingSettings
    web: WebSettings

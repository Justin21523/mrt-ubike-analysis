from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional


Granularity = Literal["15min", "hour", "day"]
SpatialJoinMethod = Literal["buffer", "nearest"]


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
    logging: LoggingSettings
    web: WebSettings


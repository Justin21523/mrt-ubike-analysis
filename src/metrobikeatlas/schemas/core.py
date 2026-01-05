from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class MetroStation:
    station_id: str
    name: str
    lat: float
    lon: float
    city: str
    system: str = "METRO"
    name_en: Optional[str] = None


@dataclass(frozen=True)
class BikeStation:
    station_id: str
    name: str
    lat: float
    lon: float
    city: str
    operator: str = "BIKE"
    capacity: Optional[int] = None


@dataclass(frozen=True)
class BikeAvailability:
    station_id: str
    ts: datetime
    available_bikes: int
    available_docks: Optional[int]
    source: str


@dataclass(frozen=True)
class StationBikeLink:
    metro_station_id: str
    bike_station_id: str
    distance_m: float


@dataclass(frozen=True)
class TimeSeriesPoint:
    ts: datetime
    value: float


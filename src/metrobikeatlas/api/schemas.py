from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class StationOut(BaseModel):
    id: str
    name: str
    lat: float
    lon: float
    city: Optional[str] = None
    system: Optional[str] = None


class TimeSeriesPointOut(BaseModel):
    ts: datetime
    value: float


class MetricSeriesOut(BaseModel):
    metric: str = Field(..., examples=["metro_ridership_proxy", "bike_available_bikes"])
    points: list[TimeSeriesPointOut]


class StationTimeSeriesOut(BaseModel):
    station_id: str
    granularity: str
    timezone: str
    series: list[MetricSeriesOut]


class NearbyBikeOut(BaseModel):
    id: str
    name: str
    lat: float
    lon: float
    distance_m: float
    capacity: Optional[int] = None


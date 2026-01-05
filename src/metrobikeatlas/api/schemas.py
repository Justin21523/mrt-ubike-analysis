from __future__ import annotations

from datetime import datetime
from typing import Optional, Union

from pydantic import BaseModel, Field


class StationOut(BaseModel):
    id: str
    name: str
    lat: float
    lon: float
    city: Optional[str] = None
    system: Optional[str] = None
    district: Optional[str] = None
    cluster: Optional[int] = None


class TimeSeriesPointOut(BaseModel):
    ts: datetime
    value: float


class MetricSeriesOut(BaseModel):
    metric: str = Field(..., examples=["metro_ridership", "metro_flow_proxy_from_bike_rent"])
    points: list[TimeSeriesPointOut]
    source: Optional[str] = Field(default=None, examples=["metro_ridership", "bike_proxy"])
    is_proxy: bool = False


class StationTimeSeriesOut(BaseModel):
    station_id: str
    granularity: str
    timezone: str
    series: list[MetricSeriesOut]


class FactorOut(BaseModel):
    name: str
    value: Union[float, int, str, None]
    percentile: Optional[float] = None


class StationFactorsOut(BaseModel):
    station_id: str
    available: bool = True
    factors: list[FactorOut]


class SimilarStationOut(BaseModel):
    id: str
    name: Optional[str] = None
    distance: float
    cluster: Optional[int] = None


class CorrelationOut(BaseModel):
    feature: str
    correlation: float
    n: int


class RegressionCoefficientOut(BaseModel):
    feature: str
    coefficient: float


class RegressionOut(BaseModel):
    r2: Optional[float] = None
    n: Optional[int] = None
    coefficients: list[RegressionCoefficientOut] = []


class AnalyticsOverviewOut(BaseModel):
    available: bool
    correlations: list[CorrelationOut] = []
    regression: Optional[RegressionOut] = None
    clusters: Optional[dict[int, int]] = None


class NearbyBikeOut(BaseModel):
    id: str
    name: str
    lat: float
    lon: float
    distance_m: float
    capacity: Optional[int] = None

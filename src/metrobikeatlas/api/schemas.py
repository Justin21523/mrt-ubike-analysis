from __future__ import annotations

from datetime import datetime
from typing import Optional, Union

from pydantic import BaseModel, Field


class TemporalConfigOut(BaseModel):
    timezone: str
    granularity: str


class SpatialConfigOut(BaseModel):
    join_method: str
    radius_m: float
    nearest_k: int


class SimilarityConfigOut(BaseModel):
    top_k: int
    metric: str
    standardize: bool


class ClusteringConfigOut(BaseModel):
    k: int
    standardize: bool


class AnalyticsConfigOut(BaseModel):
    similarity: SimilarityConfigOut
    clustering: ClusteringConfigOut


class WebMapConfigOut(BaseModel):
    center_lat: float
    center_lon: float
    zoom: int


class AppConfigOut(BaseModel):
    app_name: str
    demo_mode: bool
    temporal: TemporalConfigOut
    spatial: SpatialConfigOut
    analytics: AnalyticsConfigOut
    web_map: WebMapConfigOut


class StationOut(BaseModel):
    id: str
    name: str
    lat: float
    lon: float
    city: Optional[str] = None
    system: Optional[str] = None
    district: Optional[str] = None
    cluster: Optional[int] = None
    source: Optional[str] = Field(default=None, examples=["demo", "silver", "external_csv"])

class StationsResponseOut(BaseModel):
    items: list[StationOut] = Field(default_factory=list)
    meta: dict[str, object] = Field(default_factory=dict)


class MetaOut(BaseModel):
    now_utc: datetime
    demo_mode: bool
    meta: dict[str, object] = Field(default_factory=dict)
    silver_build_meta: Optional[dict[str, object]] = None
    collector_heartbeat: Optional[dict[str, object]] = None
    bronze: dict[str, object] = Field(default_factory=dict)


class ReplayOut(BaseModel):
    station_id: str
    meta: dict[str, object] = Field(default_factory=dict)
    stations: Optional[StationsResponseOut] = None
    timeseries: Optional[StationTimeSeriesOut] = None
    nearby_bike: Optional[NearbyBikeResponseOut] = None


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
    meta: dict[str, object] = Field(default_factory=dict)


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


class NearbyBikeResponseOut(BaseModel):
    station_id: str
    items: list[NearbyBikeOut] = Field(default_factory=list)
    meta: dict[str, object] = Field(default_factory=dict)


class BikeStationOut(BaseModel):
    id: str
    name: str
    lat: float
    lon: float
    city: Optional[str] = None
    operator: Optional[str] = None
    capacity: Optional[int] = None


class FileStatusOut(BaseModel):
    path: str
    exists: bool
    mtime_utc: Optional[datetime] = None
    size_bytes: Optional[int] = None


class DatasetStatusOut(BaseModel):
    label: str
    dir: str
    file_count: int = 0
    latest_file: Optional[FileStatusOut] = None


class CollectorStatusOut(BaseModel):
    pid_path: str
    log_path: str
    running: bool = False
    pid: Optional[int] = None
    log_tail: list[str] = []


class AppStatusOut(BaseModel):
    now_utc: datetime
    demo_mode: bool
    bronze_dir: str
    silver_dir: str
    tdx: dict[str, object] = {}
    health: dict[str, object] = {}
    silver_tables: list[FileStatusOut] = []
    bronze_datasets: list[DatasetStatusOut] = []
    collector: Optional[CollectorStatusOut] = None
    alerts: list["AlertOut"] = []
    metro_tdx_404_count: int = 0
    metro_tdx_404_last_utc: Optional[datetime] = None


class MetroAvailabilityPointOut(BaseModel):
    station_id: str
    ts: datetime
    available_bikes_total: float


class MetroHeatPointOut(BaseModel):
    station_id: str
    ts: datetime
    metric: str = Field(..., examples=["available", "rent_proxy", "return_proxy"])
    agg: str = Field(..., examples=["sum", "mean"])
    value: float


class TimeIndexOut(BaseModel):
    timestamps: list[datetime]


class HeatIndexResponseOut(BaseModel):
    timestamps: list[datetime]
    meta: dict[str, object] = Field(default_factory=dict)


class HeatAtResponseOut(BaseModel):
    ts: datetime
    metric: str = Field(..., examples=["available", "rent_proxy", "return_proxy"])
    agg: str = Field(..., examples=["sum", "mean"])
    points: list[MetroHeatPointOut] = Field(default_factory=list)
    meta: dict[str, object] = Field(default_factory=dict)


class AlertOut(BaseModel):
    level: str = Field(..., examples=["info", "warning", "error", "critical"])
    title: str
    message: str
    commands: list[str] = []


class CollectorStartIn(BaseModel):
    availability_interval_seconds: int = 600
    stations_refresh_interval_hours: float = 24.0
    jitter_seconds: float = 5.0
    build_silver_interval_seconds: Optional[int] = 1800


class AdminActionOut(BaseModel):
    ok: bool = True
    detail: Optional[str] = None
    pid: Optional[int] = None
    meta: dict[str, object] = Field(default_factory=dict)
    duration_s: Optional[float] = None
    artifacts: list[FileStatusOut] = []
    stdout_tail: list[str] = []
    job_id: Optional[str] = None


class JobOut(BaseModel):
    id: str
    kind: str = Field(..., examples=["build_silver"])
    status: str = Field(..., examples=["queued", "running", "succeeded", "failed", "canceled", "unknown"])
    stage: Optional[str] = Field(default=None, examples=["starting", "metro_stations", "bike_stations", "bike_timeseries", "links", "done"])
    progress_pct: Optional[int] = Field(default=None, examples=[0, 25, 50, 75, 100])
    created_at_utc: datetime
    started_at_utc: Optional[datetime] = None
    finished_at_utc: Optional[datetime] = None
    pid: Optional[int] = None
    returncode: Optional[int] = None
    log_path: Optional[str] = None
    stdout_tail: list[str] = []
    command: Optional[list[str]] = None


class JobRerunIn(BaseModel):
    # Optional raw CLI args to append to the rerun command.
    args: list[str] = Field(default_factory=list)
    # Common overrides (translated into build_silver.py CLI flags).
    bronze_dir: Optional[str] = None
    silver_dir: Optional[str] = None
    max_availability_files: Optional[int] = None
    external_metro_stations_csv: Optional[str] = None
    prefer_external_metro: Optional[bool] = None


class JobEventOut(BaseModel):
    ts_utc: Optional[datetime] = None
    level: str = "info"
    stage: Optional[str] = None
    progress_pct: Optional[int] = None
    message: Optional[str] = None
    artifacts: list[dict[str, object]] = Field(default_factory=list)
    raw: dict[str, object] = Field(default_factory=dict)


class JobEventsOut(BaseModel):
    job_id: str
    kind: str
    events: list[JobEventOut] = Field(default_factory=list)
    latest: Optional[JobEventOut] = None
    artifacts: list[dict[str, object]] = Field(default_factory=list)


class ExternalCsvIssueOut(BaseModel):
    level: str = Field(..., examples=["error", "warning"])
    message: str


class ExternalValidationOut(BaseModel):
    ok: bool = True
    path: str
    row_count: int = 0
    issues: list[ExternalCsvIssueOut] = Field(default_factory=list)
    head: list[dict[str, object]] = Field(default_factory=list)


class ExternalPreviewOut(BaseModel):
    ok: bool = True
    path: str
    row_count: int = 0
    issues: list[ExternalCsvIssueOut] = Field(default_factory=list)
    columns: list[str] = Field(default_factory=list)
    rows: list[dict[str, object]] = Field(default_factory=list)


class HotspotStationOut(BaseModel):
    station_id: str
    name: Optional[str] = None
    value: float
    rank: int
    reason: Optional[str] = None


class HotspotsOut(BaseModel):
    metric: str
    agg: str
    ts: datetime
    hot: list[HotspotStationOut] = Field(default_factory=list)
    cold: list[HotspotStationOut] = Field(default_factory=list)
    explanation: str = ""


class BriefingSnapshotIn(BaseModel):
    station_id: Optional[str] = None
    story_step: Optional[str] = None
    kpis: list[dict[str, object]] = Field(default_factory=list)
    settings: dict[str, object] = Field(default_factory=dict)
    artifacts: dict[str, object] = Field(default_factory=dict)
    policy_cards: list[dict[str, object]] = Field(default_factory=list)
    notes: Optional[str] = None


class BriefingSnapshotOut(BaseModel):
    id: str
    created_at_utc: datetime
    snapshot: BriefingSnapshotIn

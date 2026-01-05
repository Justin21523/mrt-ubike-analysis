from __future__ import annotations

from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Request

from metrobikeatlas.api.schemas import (
    AnalyticsOverviewOut,
    AppConfigOut,
    BikeStationOut,
    NearbyBikeOut,
    SimilarStationOut,
    StationFactorsOut,
    StationOut,
    StationTimeSeriesOut,
)
from metrobikeatlas.api.service import StationService


router = APIRouter()


def get_service(request: Request) -> StationService:
    return request.app.state.station_service  # type: ignore[attr-defined]


@router.get("/config", response_model=AppConfigOut)
def get_config(service: StationService = Depends(get_service)) -> AppConfigOut:
    cfg = service.config
    return AppConfigOut(
        app_name=cfg.app.name,
        demo_mode=cfg.app.demo_mode,
        temporal={
            "timezone": cfg.temporal.timezone,
            "granularity": cfg.temporal.granularity,
        },
        spatial={
            "join_method": cfg.spatial.join_method,
            "radius_m": cfg.spatial.radius_m,
            "nearest_k": cfg.spatial.nearest_k,
        },
        analytics={
            "similarity": {
                "top_k": cfg.analytics.similarity.top_k,
                "metric": cfg.analytics.similarity.metric,
                "standardize": cfg.analytics.similarity.standardize,
            },
            "clustering": {
                "k": cfg.analytics.clustering.k,
                "standardize": cfg.analytics.clustering.standardize,
            },
        },
        web_map={
            "center_lat": cfg.web.map.center_lat,
            "center_lon": cfg.web.map.center_lon,
            "zoom": cfg.web.map.zoom,
        },
    )


@router.get("/bike_stations", response_model=list[BikeStationOut])
def list_bike_stations(service: StationService = Depends(get_service)) -> list[BikeStationOut]:
    bikes = service.list_bike_stations()
    return [
        BikeStationOut(
            id=s["station_id"],
            name=s["name"],
            lat=s["lat"],
            lon=s["lon"],
            city=s.get("city"),
            operator=s.get("operator"),
            capacity=s.get("capacity"),
        )
        for s in bikes
    ]


@router.get("/stations", response_model=list[StationOut])
def list_stations(service: StationService = Depends(get_service)) -> list[StationOut]:
    stations = service.list_stations()
    return [
        StationOut(
            id=s["station_id"],
            name=s["name"],
            lat=s["lat"],
            lon=s["lon"],
            city=s.get("city"),
            system=s.get("system"),
            district=s.get("district"),
            cluster=s.get("cluster"),
        )
        for s in stations
    ]


@router.get("/station/{station_id}/timeseries", response_model=StationTimeSeriesOut)
def station_timeseries(
    station_id: str,
    join_method: Optional[Literal["buffer", "nearest"]] = None,
    radius_m: Optional[float] = None,
    nearest_k: Optional[int] = None,
    granularity: Optional[Literal["15min", "hour", "day"]] = None,
    timezone: Optional[str] = None,
    window_days: Optional[int] = None,
    metro_series: Literal["auto", "ridership", "proxy"] = "auto",
    service: StationService = Depends(get_service),
) -> StationTimeSeriesOut:
    try:
        payload = service.station_timeseries(
            station_id,
            join_method=join_method,
            radius_m=radius_m,
            nearest_k=nearest_k,
            granularity=granularity,
            timezone=timezone,
            window_days=window_days,
            metro_series=metro_series,
        )
    except KeyError:
        raise HTTPException(status_code=404, detail="Station not found")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return StationTimeSeriesOut.model_validate(payload)


@router.get("/station/{station_id}/nearby_bike", response_model=list[NearbyBikeOut])
def nearby_bike(
    station_id: str,
    join_method: Optional[Literal["buffer", "nearest"]] = None,
    radius_m: Optional[float] = None,
    nearest_k: Optional[int] = None,
    limit: Optional[int] = None,
    service: StationService = Depends(get_service),
) -> list[NearbyBikeOut]:
    try:
        payload = service.nearby_bike(
            station_id,
            join_method=join_method,
            radius_m=radius_m,
            nearest_k=nearest_k,
            limit=limit,
        )
    except KeyError:
        raise HTTPException(status_code=404, detail="Station not found")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return [
        NearbyBikeOut(
            id=s["station_id"],
            name=s["name"],
            lat=s["lat"],
            lon=s["lon"],
            distance_m=s["distance_m"],
            capacity=s.get("capacity"),
        )
        for s in payload
    ]


@router.get("/station/{station_id}/factors", response_model=StationFactorsOut)
def station_factors(
    station_id: str, service: StationService = Depends(get_service)
) -> StationFactorsOut:
    try:
        payload = service.station_factors(station_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Station not found")
    return StationFactorsOut.model_validate(payload)


@router.get("/station/{station_id}/similar", response_model=list[SimilarStationOut])
def similar_stations(
    station_id: str,
    top_k: Optional[int] = None,
    metric: Optional[Literal["euclidean", "cosine"]] = None,
    standardize: Optional[bool] = None,
    service: StationService = Depends(get_service),
) -> list[SimilarStationOut]:
    try:
        payload = service.similar_stations(
            station_id,
            top_k=top_k,
            metric=metric,
            standardize=standardize,
        )
    except KeyError:
        raise HTTPException(status_code=404, detail="Station not found")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return [
        SimilarStationOut(
            id=s["station_id"],
            name=s.get("name"),
            distance=s["distance"],
            cluster=s.get("cluster"),
        )
        for s in payload
    ]


@router.get("/analytics/overview", response_model=AnalyticsOverviewOut)
def analytics_overview(service: StationService = Depends(get_service)) -> AnalyticsOverviewOut:
    payload = service.analytics_overview()
    return AnalyticsOverviewOut.model_validate(payload)

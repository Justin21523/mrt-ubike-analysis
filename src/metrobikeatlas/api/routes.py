from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request

from metrobikeatlas.api.schemas import (
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
    station_id: str, service: StationService = Depends(get_service)
) -> StationTimeSeriesOut:
    try:
        payload = service.station_timeseries(station_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Station not found")
    return StationTimeSeriesOut.model_validate(payload)


@router.get("/station/{station_id}/nearby_bike", response_model=list[NearbyBikeOut])
def nearby_bike(
    station_id: str, service: StationService = Depends(get_service)
) -> list[NearbyBikeOut]:
    try:
        payload = service.nearby_bike(station_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Station not found")
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
    station_id: str, service: StationService = Depends(get_service)
) -> list[SimilarStationOut]:
    try:
        payload = service.similar_stations(station_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Station not found")
    return [
        SimilarStationOut(
            id=s["station_id"],
            name=s.get("name"),
            distance=s["distance"],
            cluster=s.get("cluster"),
        )
        for s in payload
    ]
